package com.example.gsb.batcher;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

class OrderingTest {

    /** 大 flushInterval，保证测试期间只有显式触发，行为确定。 */
    private static BatcherConfig.Builder deterministic() {
        return BatcherConfig.builder()
                .flushInterval(Duration.ofHours(1))
                .maxBufferedItems(1_000_000);
    }

    @Test
    void sameThreadWritesFlushInCommitOrder() {
        TestSupport.RecordingSink<Integer> sink = new TestSupport.RecordingSink<>();
        try (ThreadLocalBatcher<Integer> batcher =
                new ThreadLocalBatcher<>(deterministic().maxBatchSize(10).build(), sink)) {
            for (int i = 0; i < 95; i++) {
                batcher.write(i);
            }
            batcher.flushAll();

            List<Integer> expected = new ArrayList<>();
            for (int i = 0; i < 95; i++) {
                expected.add(i);
            }
            assertThat(sink.allItems()).containsExactlyElementsOf(expected);
            // 每批（除最后一批）都正好 10 条，批内顺序即提交顺序
            assertThat(sink.batches).hasSize(10);
            assertThat(sink.batches.subList(0, 9)).allSatisfy(b -> assertThat(b.items())
                    .hasSize(10));
        }
    }

    @Test
    void perThreadOrderPreservedUnderConcurrency() throws Exception {
        TestSupport.RecordingSink<String> sink = new TestSupport.RecordingSink<>();
        int threads = 4;
        int perThread = 250;
        try (ThreadLocalBatcher<String> batcher =
                new ThreadLocalBatcher<>(deterministic().maxBatchSize(7).build(), sink)) {
            CountDownLatch start = new CountDownLatch(1);
            List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                Thread w = new Thread(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    for (int i = 0; i < perThread; i++) {
                        batcher.write("t" + id + "-" + i);
                    }
                }, "writer-" + id);
                workers.add(w);
                w.start();
            }
            start.countDown();
            for (Thread w : workers) {
                w.join();
            }
            batcher.flushAll();

            // 总量不丢不重
            assertThat(sink.allItems()).hasSize(threads * perThread);
            // 每个线程自己的写入在全局输出中保持提交顺序（不同线程之间可交错）
            Map<String, List<String>> byThread = new HashMap<>();
            for (String item : sink.allItems()) {
                byThread.computeIfAbsent(item.substring(0, item.indexOf('-')), k -> new ArrayList<>())
                        .add(item);
            }
            for (int t = 0; t < threads; t++) {
                List<String> expected = new ArrayList<>();
                for (int i = 0; i < perThread; i++) {
                    expected.add("t" + t + "-" + i);
                }
                assertThat(byThread.get("t" + t))
                        .as("thread t%s order", t)
                        .containsExactlyElementsOf(expected);
            }
            // 同一 owner 的批次序号单调递增
            Map<Long, Long> lastSeq = new HashMap<>();
            synchronized (sink.batches) {
                for (Batch<String> b : sink.batches) {
                    long prev = lastSeq.getOrDefault(b.ownerId(), -1L);
                    assertThat(b.sequence()).isGreaterThan(prev);
                    lastSeq.put(b.ownerId(), b.sequence());
                }
            }
        }
    }

    @Test
    void batchSizeTriggersFlush() {
        TestSupport.RecordingSink<Integer> sink = new TestSupport.RecordingSink<>();
        try (ThreadLocalBatcher<Integer> batcher =
                new ThreadLocalBatcher<>(deterministic().maxBatchSize(3).build(), sink)) {
            batcher.write(1);
            batcher.write(2);
            assertThat(sink.batches).isEmpty();
            batcher.write(3);
            assertThat(sink.batches).hasSize(1);
            assertThat(sink.batches.get(0).items()).containsExactly(1, 2, 3);
        }
    }

    @Test
    void timeoutTriggersFlush() {
        TestSupport.RecordingSink<String> sink = new TestSupport.RecordingSink<>();
        BatcherConfig config = BatcherConfig.builder()
                .maxBatchSize(1000)
                .flushInterval(Duration.ofMillis(50))
                .build();
        try (ThreadLocalBatcher<String> batcher = new ThreadLocalBatcher<>(config, sink)) {
            batcher.write("lonely");
            TestSupport.awaitUntil(() -> sink.allItems().size() == 1, Duration.ofSeconds(5));
            assertThat(sink.allItems()).containsExactly("lonely");
        }
    }

    @Test
    void sequencesAreMonotonicPerOwnerAcrossFlushes() {
        TestSupport.RecordingSink<Integer> sink = new TestSupport.RecordingSink<>();
        try (ThreadLocalBatcher<Integer> batcher =
                new ThreadLocalBatcher<>(deterministic().maxBatchSize(2).build(), sink)) {
            for (int i = 0; i < 6; i++) {
                batcher.write(i);
            }
            assertThat(sink.batches).hasSize(3);
            assertThat(sink.batches)
                    .extracting(Batch::sequence)
                    .containsExactly(0L, 1L, 2L);
        }
    }
}
