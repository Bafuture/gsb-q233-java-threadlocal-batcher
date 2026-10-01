package com.example.gsb.batcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class MemoryLimitTest {

    private static BatcherConfig.Builder deterministic() {
        return BatcherConfig.builder()
                .flushInterval(Duration.ofHours(1))
                .maxBufferedItems(1_000_000);
    }

    @Test
    void exceedingLimitFlushesCurrentThreadFirst() {
        TestSupport.RecordingSink<Integer> sink = new TestSupport.RecordingSink<>();
        BatcherConfig config = deterministic()
                .maxBatchSize(100) // 批量阈值远大于上限，强制由内存上限触发刷出
                .maxBufferedItems(5)
                .build();
        try (ThreadLocalBatcher<Integer> batcher = new ThreadLocalBatcher<>(config, sink)) {
            for (int i = 0; i < 5; i++) {
                batcher.write(i);
            }
            assertThat(sink.batches).as("尚未超限，不应刷出").isEmpty();
            assertThat(batcher.stats().globalBuffered()).isEqualTo(5);

            batcher.write(6); // 第 6 条超限 -> 先刷出本线程已有的 5 条
            assertThat(sink.batches).hasSize(1);
            assertThat(sink.batches.get(0).items()).containsExactly(0, 1, 2, 3, 4);
            assertThat(batcher.stats().globalBuffered()).isEqualTo(1);
        }
    }

    @Test
    void writeIsRejectedWhenFlushingCannotFreeSpace() throws Exception {
        TestSupport.RecordingSink<Integer> sink = new TestSupport.RecordingSink<>();
        BatcherConfig config = deterministic()
                .maxBatchSize(100)
                .maxBufferedItems(2)
                .build();
        try (ThreadLocalBatcher<Integer> batcher = new ThreadLocalBatcher<>(config, sink)) {
            batcher.write(1);
            batcher.write(2); // 主线程占满 2 个名额

            Thread blocker = new Thread(() -> {
                assertThatThrownBy(() -> batcher.write(99))
                        .isInstanceOf(WriteRejectedException.class);
            }, "blocked-writer");
            blocker.start();
            blocker.join();

            assertThat(batcher.stats().rejectedCount()).isEqualTo(1);
            // 被拒后缓冲仍只有主线程的 2 条，不增长
            assertThat(batcher.stats().globalBuffered()).isEqualTo(2);

            batcher.flushAll();
            assertThat(sink.allItems()).containsExactly(1, 2);
            assertThat(batcher.stats().globalBuffered()).isZero();
        }
    }
}
