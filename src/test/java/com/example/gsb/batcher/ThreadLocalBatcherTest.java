package com.example.gsb.batcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class ThreadLocalBatcherTest {

    private static void awaitFlushes(RecordingSink<?> sink, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (sink.batches.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(sink.batches).hasSizeGreaterThanOrEqualTo(expected);
    }

    @Test
    void batchSizeTriggersFlushInOrder() throws Exception {
        RecordingSink<Integer> sink = new RecordingSink<>();
        try (ThreadLocalBatcher<Integer> batcher = ThreadLocalBatcher
                .<Integer>builder(sink)
                .batchSize(10)
                .flushInterval(Duration.ofSeconds(30))
                .maxGlobalBuffered(100)
                .build()) {

            for (int i = 0; i < 25; i++) {
                batcher.write(i);
            }
            awaitFlushes(sink, 2);

            assertThat(sink.batches).hasSize(2);
            assertThat(sink.batches.get(0).items()).containsExactly(range(0, 10));
            assertThat(sink.batches.get(1).items()).containsExactly(range(10, 20));
            // 剩余 5 条仍在缓冲，flushAll 后成为第 3 批，顺序不变
            FlushAllResult result = batcher.flushAll(Duration.ofSeconds(2));
            assertThat(result.complete()).isTrue();
            assertThat(sink.batches).hasSize(3);
            assertThat(sink.batches.get(2).items()).containsExactly(range(20, 25));

            long threadId = Thread.currentThread().getId();
            for (int i = 0; i < sink.batches.size(); i++) {
                FlushBatch<Integer> batch = sink.batches.get(i);
                assertThat(batch.ownerThreadId()).isEqualTo(threadId);
                assertThat(batch.sequence()).isEqualTo(i);
            }
            assertThat(sink.allItems()).containsExactly(range(0, 25));
        }
    }

    @Test
    void timeoutTriggersFlush() throws Exception {
        RecordingSink<String> sink = new RecordingSink<>();
        try (ThreadLocalBatcher<String> batcher = ThreadLocalBatcher
                .<String>builder(sink)
                .batchSize(1000)
                .flushInterval(Duration.ofMillis(60))
                .maxGlobalBuffered(1000)
                .build()) {

            batcher.write("a");
            awaitFlushes(sink, 1);
            assertThat(sink.batches.get(0).items()).containsExactly("a");
        }
    }

    @Test
    void perThreadOrderPreservedAcrossThreads() throws Exception {
        RecordingSink<Integer> sink = new RecordingSink<>();
        int threads = 4;
        int perThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try (ThreadLocalBatcher<Integer> batcher = ThreadLocalBatcher
                .<Integer>builder(sink)
                .batchSize(7)
                .flushInterval(Duration.ofSeconds(30))
                .maxGlobalBuffered(10_000)
                .build()) {

            CountDownLatch done = new CountDownLatch(threads);
            for (int t = 0; t < threads; t++) {
                final int base = t * 1000;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            batcher.write(base + i);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            FlushAllResult result = batcher.flushAll(Duration.ofSeconds(5));
            assertThat(result.complete()).isTrue();
        } finally {
            pool.shutdownNow();
        }

        Map<Long, List<Integer>> byThread = new java.util.HashMap<>();
        Map<Long, List<Long>> seqsByThread = new java.util.HashMap<>();
        for (FlushBatch<Integer> batch : sink.batches) {
            byThread.computeIfAbsent(batch.ownerThreadId(), k -> new ArrayList<>())
                    .addAll(batch.items());
            seqsByThread.computeIfAbsent(batch.ownerThreadId(), k -> new ArrayList<>())
                    .add(batch.sequence());
        }
        assertThat(byThread).hasSize(threads);
        byThread.forEach((id, items) -> assertThat(items)
                .as("thread %d items in submission order", id)
                .isSorted());
        seqsByThread.forEach((id, seqs) -> assertThat(seqs)
                .as("thread %d sequence strictly increasing from 0", id)
                .containsExactly(seqsSorted(seqs.size())));

        // 跨线程顺序不做保证，但全部数据恰好刷出一次
        assertThat(sink.allItems()).hasSize(threads * perThread);
        assertThat(sink.allItems()).doesNotHaveDuplicates();
    }

    @Test
    void rejectWhenGlobalBufferLimitReached() throws Exception {
        RecordingSink<Integer> sink = new RecordingSink<>();
        try (ThreadLocalBatcher<Integer> batcher = ThreadLocalBatcher
                .<Integer>builder(sink)
                .batchSize(100)
                .flushInterval(Duration.ofSeconds(30))
                .maxGlobalBuffered(5)
                .overflowPolicy(OverflowPolicy.REJECT)
                .build()) {

            for (int i = 0; i < 5; i++) {
                batcher.write(i);
            }
            assertThatThrownBy(() -> batcher.write(5))
                    .isInstanceOf(WriteRejectedException.class);

            BatcherStats stats = batcher.stats();
            assertThat(stats.rejectedCount()).isEqualTo(1);
            assertThat(stats.itemsWritten()).isEqualTo(5);
            assertThat(stats.bufferDepths().get(Thread.currentThread().getName())).isEqualTo(5);
            assertThat(stats.flushCount()).isZero();
        }
    }

    @Test
    void flushPolicyDrainsBufferWhenLimitReached() throws Exception {
        RecordingSink<Integer> sink = new RecordingSink<>();
        try (ThreadLocalBatcher<Integer> batcher = ThreadLocalBatcher
                .<Integer>builder(sink)
                .batchSize(100)
                .flushInterval(Duration.ofSeconds(30))
                .maxGlobalBuffered(3)
                .overflowPolicy(OverflowPolicy.FLUSH)
                .build()) {

            for (int i = 0; i < 10; i++) {
                batcher.write(i);
            }
            batcher.flushAll(Duration.ofSeconds(2));

            assertThat(sink.allItems()).containsExactly(range(0, 10));
            // 每次到上限即提前刷出，缓冲深度从未超过阈值
            BatcherStats stats = batcher.stats();
            assertThat(stats.rejectedCount()).isZero();
            assertThat(stats.bufferDepths()).isEmpty();
        }
    }

    @Test
    void duplicateFlushesAreIdempotent() {
        List<Integer> delivered = new ArrayList<>();
        IdempotentSink<Integer> dedup = new IdempotentSink<>(batch -> delivered.addAll(batch.items()));

        FlushBatch<Integer> batch1 = new FlushBatch<>(1, "t1", 0, List.of(1, 2, 3),
                java.time.Instant.now());
        FlushBatch<Integer> batch2 = new FlushBatch<>(1, "t1", 1, List.of(4, 5),
                java.time.Instant.now());

        dedup.flush(batch1);
        dedup.flush(batch1); // 崩溃后重复刷出
        dedup.flush(batch2);
        dedup.flush(batch1);
        dedup.flush(batch2);

        assertThat(delivered).containsExactly(1, 2, 3, 4, 5);
        assertThat(dedup.deduplicatedCount()).isEqualTo(2);
    }

    @Test
    void failedFlushIsRetriedWithSameSequence() {
        RecordingSink<Integer> sink = new RecordingSink<>();
        AtomicBoolean failedOnce = new AtomicBoolean();
        BatchSink<Integer> flaky = batch -> {
            if (failedOnce.compareAndSet(false, true)) {
                throw new RuntimeException("downstream down");
            }
            sink.flush(batch);
        };
        try (ThreadLocalBatcher<Integer> batcher = ThreadLocalBatcher
                .<Integer>builder(flaky)
                .batchSize(3)
                .flushInterval(Duration.ofSeconds(30))
                .maxGlobalBuffered(100)
                .build()) {

            batcher.write(1);
            batcher.write(2);
            assertThatThrownBy(() -> batcher.write(3)).isInstanceOf(FlushException.class);
            // 批次已放回缓冲，重试仍带相同序号
            batcher.flush();

            assertThat(sink.batches).hasSize(1);
            FlushBatch<Integer> delivered = sink.batches.get(0);
            assertThat(delivered.sequence()).isZero();
            assertThat(delivered.items()).containsExactly(1, 2, 3);
        }
    }

    @Test
    void flushAllReportsPendingThreadsOnTimeout() throws Exception {
        RecordingSink<Integer> sink = new RecordingSink<>();
        sink.sleepMillis = 300;
        CountDownLatch threadAlive = new CountDownLatch(1);
        try (ThreadLocalBatcher<Integer> batcher = ThreadLocalBatcher
                .<Integer>builder(sink)
                .batchSize(100)
                .flushInterval(Duration.ofSeconds(30))
                .maxGlobalBuffered(100)
                .build()) {

            Thread worker = new Thread(() -> {
                batcher.write(100);
                try {
                    threadAlive.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "slow-worker");
            worker.start();
            waitUntilDepth(batcher, 1, 5_000);
            batcher.write(200); // 主线程缓冲也有一条

            FlushAllResult result = batcher.flushAll(Duration.ofMillis(120));

            assertThat(result.complete()).isFalse();
            assertThat(result.timedOut()).isTrue();
            assertThat(result.pendingThreads()).hasSize(1);
            assertThat(result.flushedThreads()).hasSize(1);
            long pendingId = result.pendingThreads().get(0).threadId();
            long flushedId = result.flushedThreads().get(0).threadId();
            assertThat(pendingId).isNotEqualTo(flushedId);

            threadAlive.countDown();
            worker.join(2_000);
        }
    }

    @Test
    void deadThreadBufferIsAutoFlushed() throws Exception {
        RecordingSink<Integer> sink = new RecordingSink<>();
        try (ThreadLocalBatcher<Integer> batcher = ThreadLocalBatcher
                .<Integer>builder(sink)
                .batchSize(1000)
                .flushInterval(Duration.ofSeconds(30))
                .maxGlobalBuffered(1000)
                .build()) {

            Thread worker = new Thread(() -> {
                batcher.write(1);
                batcher.write(2);
                batcher.write(3);
            }, "dying-worker");
            worker.start();
            worker.join(2_000);
            awaitFlushes(sink, 1);

            assertThat(sink.batches.get(0).ownerThreadName()).isEqualTo("dying-worker");
            assertThat(sink.batches.get(0).items()).containsExactly(1, 2, 3);
        }
    }

    @Test
    void poolThreadReclamationLosesNoData() throws Exception {
        RecordingSink<Integer> sink = new RecordingSink<>();
        ThreadLocalBatcher<Integer> batcher = ThreadLocalBatcher
                .<Integer>builder(sink)
                .batchSize(1000)
                .flushInterval(Duration.ofSeconds(30))
                .maxGlobalBuffered(1000)
                .build();

        ExecutorService pool = Executors.newFixedThreadPool(1);
        CountDownLatch taskDone = new CountDownLatch(1);
        pool.submit(() -> {
            batcher.write(7);
            batcher.write(8);
            taskDone.countDown();
        });
        assertThat(taskDone.await(2, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();
        assertThat(pool.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        // 池线程已死，reaper 必须把其缓冲刷出
        awaitFlushes(sink, 1);
        assertThat(sink.allItems()).containsExactly(7, 8);

        batcher.close();
    }

    @Test
    void statsTrackFlushesMergeRatioAndDepths() throws Exception {
        RecordingSink<Integer> sink = new RecordingSink<>();
        try (ThreadLocalBatcher<Integer> batcher = ThreadLocalBatcher
                .<Integer>builder(sink)
                .batchSize(4)
                .flushInterval(Duration.ofSeconds(30))
                .maxGlobalBuffered(100)
                .build()) {

            for (int i = 0; i < 8; i++) {
                batcher.write(i);
            }
            BatcherStats stats = batcher.stats();
            assertThat(stats.flushCount()).isEqualTo(2);
            assertThat(stats.itemsWritten()).isEqualTo(8);
            assertThat(stats.itemsFlushed()).isEqualTo(8);
            assertThat(stats.mergeRatio()).isEqualTo(4.0);
            assertThat(stats.bufferDepths()).isEmpty();
            assertThat(stats.rejectedCount()).isZero();

            batcher.write(9);
            BatcherStats withDepth = batcher.stats();
            assertThat(withDepth.bufferDepths()).containsEntry(Thread.currentThread().getName(), 1);
        }
    }

    // ---- helpers ----

    private static Integer[] range(int fromInclusive, int exclusive) {
        Integer[] out = new Integer[exclusive - fromInclusive];
        for (int i = 0; i < out.length; i++) {
            out[i] = fromInclusive + i;
        }
        return out;
    }

    private static Long[] seqsSorted(int size) {
        Long[] out = new Long[size];
        for (int i = 0; i < size; i++) {
            out[i] = (long) i;
        }
        return out;
    }

    private static void waitUntilDepth(ThreadLocalBatcher<?> batcher, int depth, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            int total = batcher.stats().bufferDepths().values().stream()
                    .mapToInt(Integer::intValue).sum();
            if (total >= depth) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("buffer depth not reached");
    }
}
