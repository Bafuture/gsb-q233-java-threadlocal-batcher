package com.example.gsb.batcher;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class FlushAllTest {

    private static BatcherConfig.Builder deterministic() {
        return BatcherConfig.builder()
                .flushInterval(Duration.ofHours(1))
                .maxBufferedItems(1_000_000);
    }

    @Test
    void flushAllDrainsEveryThreadBuffer() throws Exception {
        TestSupport.RecordingSink<String> sink = new TestSupport.RecordingSink<>();
        try (ThreadLocalBatcher<String> batcher =
                new ThreadLocalBatcher<>(deterministic().maxBatchSize(1000).build(), sink)) {
            int threads = 5;
            List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                Thread w = new Thread(() -> {
                    for (int i = 0; i < 100; i++) {
                        batcher.write("t" + id + "-" + i);
                    }
                }, "flusher-" + id);
                workers.add(w);
                w.start();
            }
            for (Thread w : workers) {
                w.join();
            }
            assertThat(batcher.stats().globalBuffered()).isEqualTo(threads * 100L);

            FlushReport report = batcher.flushAll(Duration.ofSeconds(2));

            assertThat(report.complete()).isTrue();
            assertThat(report.unfinishedOwners()).isEmpty();
            assertThat(sink.allItems()).hasSize(threads * 100);
            assertThat(batcher.stats().globalBuffered()).isZero();
            assertThat(report.flushedOwners()).hasSize(threads);
        }
    }

    @Test
    void flushAllTimeoutReportsUnfinishedThreads() throws Exception {
        AtomicBoolean failing = new AtomicBoolean(true);
        BatchSink<String> unstable = batch -> {
            if (failing.get() && batch.items().contains("poison")) {
                throw new RuntimeException("downstream down");
            }
        };
        try (ThreadLocalBatcher<String> batcher =
                new ThreadLocalBatcher<>(deterministic().maxBatchSize(100).build(), unstable)) {
            Thread sick = new Thread(() -> batcher.write("poison"), "sick-thread");
            sick.start();
            sick.join();

            FlushReport report = batcher.flushAll(Duration.ofMillis(200));

            assertThat(report.complete()).isFalse();
            assertThat(report.unfinishedOwners())
                    .anyMatch(label -> label.startsWith("sick-thread"));
            assertThat(batcher.stats().flushFailures()).isGreaterThan(0);

            failing.set(false);
            FlushReport retry = batcher.flushAll(Duration.ofSeconds(2));
            assertThat(retry.complete()).isTrue();
        }
    }

    @Test
    void flushAllUsesConfiguredDefaultTimeout() {
        TestSupport.RecordingSink<Integer> sink = new TestSupport.RecordingSink<>();
        BatcherConfig config = deterministic()
                .flushAllTimeout(Duration.ofSeconds(1))
                .build();
        try (ThreadLocalBatcher<Integer> batcher = new ThreadLocalBatcher<>(config, sink)) {
            batcher.write(1);
            FlushReport report = batcher.flushAll();
            assertThat(report.complete()).isTrue();
        }
    }
}
