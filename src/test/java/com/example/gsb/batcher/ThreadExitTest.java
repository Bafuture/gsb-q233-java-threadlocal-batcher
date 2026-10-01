package com.example.gsb.batcher;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ThreadExitTest {

    private static BatcherConfig.Builder deterministic() {
        return BatcherConfig.builder()
                .flushInterval(Duration.ofHours(1))
                .maxBufferedItems(1_000_000)
                .maxBatchSize(1000); // 不达到批量，退出刷出是唯一触发途径
    }

    @Test
    void bufferFlushedWhenWorkerThreadExits() throws Exception {
        TestSupport.RecordingSink<String> sink = new TestSupport.RecordingSink<>();
        ThreadLocalBatcher<String> batcher =
                new ThreadLocalBatcher<>(deterministic().build(), sink);
        ExecutorService pool = Executors.newFixedThreadPool(2, batcher.threadFactory());
        for (int i = 0; i < 10; i++) {
            int id = i;
            pool.submit(() -> batcher.write("m" + id));
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        // 线程退出时的 finally 刷出可能在 join 后刚发生
        TestSupport.awaitUntil(() -> sink.allItems().size() == 10, Duration.ofSeconds(2));
        assertThat(sink.allItems()).containsExactlyInAnyOrder(
                "m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9");

        batcher.close();
    }

    @Test
    void wrappedTaskFlushesOnPooledThreadReuse() throws Exception {
        TestSupport.RecordingSink<String> sink = new TestSupport.RecordingSink<>();
        ThreadLocalBatcher<String> batcher =
                new ThreadLocalBatcher<>(deterministic().build(), sink);
        ExecutorService pool = Executors.newFixedThreadPool(1);
        for (int round = 0; round < 3; round++) {
            int r = round;
            pool.submit(batcher.wrap(() -> batcher.write("r" + r)));
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        // 同一线程被复用 3 次，每次任务结束都刷出，不需要等线程真正退出
        TestSupport.awaitUntil(() -> sink.allItems().size() == 3, Duration.ofSeconds(2));
        assertThat(sink.allItems()).containsExactlyInAnyOrder("r0", "r1", "r2");

        batcher.close();
    }

    @Test
    void closeFlushesRemainingBuffers() {
        TestSupport.RecordingSink<Integer> sink = new TestSupport.RecordingSink<>();
        ThreadLocalBatcher<Integer> batcher =
                new ThreadLocalBatcher<>(deterministic().build(), sink);
        batcher.write(7);
        batcher.close();
        assertThat(sink.allItems()).containsExactly(7);
        assertThat(batcher.stats().globalBuffered()).isZero();
    }
}
