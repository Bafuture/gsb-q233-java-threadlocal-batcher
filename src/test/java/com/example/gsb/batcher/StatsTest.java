package com.example.gsb.batcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class StatsTest {

    private static BatcherConfig.Builder deterministic() {
        return BatcherConfig.builder()
                .flushInterval(Duration.ofHours(1))
                .maxBufferedItems(1_000_000);
    }

    @Test
    void statsReportFlushCountMergeRatioRejectionsAndDepths() throws Exception {
        TestSupport.RecordingSink<Integer> sink = new TestSupport.RecordingSink<>();
        try (ThreadLocalBatcher<Integer> batcher =
                new ThreadLocalBatcher<>(deterministic()
                        .maxBatchSize(4)
                        .maxBufferedItems(20)
                        .build(), sink)) {
            for (int i = 0; i < 8; i++) {
                batcher.write(i);
            }
            BatcherStats afterBatch = batcher.stats();
            assertThat(afterBatch.flushCount()).isEqualTo(2);
            assertThat(afterBatch.itemsFlushed()).isEqualTo(8);
            assertThat(afterBatch.mergeRatio()).isEqualTo(4.0); // 平均每次刷出合并 4 条
            assertThat(afterBatch.rejectedCount()).isZero();
            assertThat(afterBatch.bufferDepths().values()).containsExactly(0);

            Thread other = new Thread(() -> {
                for (int i = 0; i < 5; i++) {
                    batcher.write(i);
                }
            }, "depth-writer");
            other.start();
            other.join();

            BatcherStats withDepth = batcher.stats();
            assertThat(withDepth.bufferDepths()).hasSize(2);
            assertThat(withDepth.bufferDepths().get("depth-writer#1")).isEqualTo(1); // 5 条中 4 条已按批量刷出
            assertThat(withDepth.globalBuffered()).isEqualTo(1);

            batcher.flushAll();
            BatcherStats afterAll = batcher.stats();
            assertThat(afterAll.flushCount()).isEqualTo(4); // 4+4(main) + 4+1(other)
            assertThat(afterAll.itemsFlushed()).isEqualTo(13);
            assertThat(afterAll.mergeRatio()).isEqualTo(13.0 / 4);
            assertThat(afterAll.bufferDepths().values()).containsExactly(0, 0);
        }
    }

    @Test
    void rejectedCountReflectsMemoryLimit() throws Exception {
        TestSupport.RecordingSink<Integer> sink = new TestSupport.RecordingSink<>();
        try (ThreadLocalBatcher<Integer> batcher =
                new ThreadLocalBatcher<>(deterministic()
                        .maxBatchSize(100)
                        .maxBufferedItems(1)
                        .build(), sink)) {
            batcher.write(1);
            Thread other = new Thread(() -> {
                for (int i = 0; i < 3; i++) {
                    final int attempt = i;
                    assertThatThrownBy(() -> batcher.write(attempt))
                            .isInstanceOf(WriteRejectedException.class);
                }
            }, "reject-writer");
            other.start();
            other.join();
            assertThat(batcher.stats().rejectedCount()).isEqualTo(3);
            assertThat(batcher.stats().globalBuffered()).isEqualTo(1);
        }
    }
}
