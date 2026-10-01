package com.example.gsb.batcher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 线程本地缓冲 + 批量合并写组件。
 *
 * 行为约定：
 * 1) 每个线程拥有独立缓冲，达到 maxBatchSize 或最老条目驻留超过 flushInterval 即刷出；
 * 2) 顺序：同一线程的写入严格按提交顺序刷出（批次序号单调递增、批内保持提交顺序）；
 *    不同线程之间的相对顺序不做任何保证；
 * 3) 内存上限：全局缓冲条数达到 maxBufferedItems 时，写入线程先刷出自己的缓冲；
 *    仍放不下则抛出 {@link WriteRejectedException}，缓冲不会无限增长；
 * 4) 崩溃安全：每个批次带 (ownerId, sequence) 幂等键；下游失败时数据保留、序号不变，
 *    重试会重发相同序号，下游用 {@link DeduplicatingSink} 去重后与只刷一次一致；
 * 5) flushAll(timeout) 刷空所有已注册线程的缓冲，超时返回未完成线程列表；
 * 6) 线程退出：用 {@link #threadFactory()} 创建的线程退出前自动刷出；
 *    线程池复用线程时用 {@link #wrap(Runnable)} 包裹任务，任务结束即刷出，回收线程不丢数据；
 * 7) close() 停止后台刷出线程并做最后一次 flushAll。
 */
public final class ThreadLocalBatcher<T> implements AutoCloseable {

    private final BatcherConfig config;
    private final BatchSink<T> sink;
    private final ConcurrentMap<Long, ThreadBuffer> buffers = new ConcurrentHashMap<>();
    private final AtomicLong bufferIds = new AtomicLong();
    private final AtomicLong globalBuffered = new AtomicLong();
    private final AtomicLong flushCount = new AtomicLong();
    private final AtomicLong itemsFlushed = new AtomicLong();
    private final AtomicLong rejectedCount = new AtomicLong();
    private final AtomicLong flushFailures = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ScheduledExecutorService scheduler;

    private final ThreadLocal<ThreadBuffer> local = ThreadLocal.withInitial(() -> {
        ThreadBuffer b = new ThreadBuffer(bufferIds.getAndIncrement(), Thread.currentThread().getName());
        buffers.put(b.id, b);
        return b;
    });

    public ThreadLocalBatcher(BatcherConfig config, BatchSink<T> sink) {
        this.config = config;
        this.sink = sink;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "thread-local-batcher-flusher");
            t.setDaemon(true);
            return t;
        });
        long scanMs = Math.max(10, config.flushInterval().toMillis() / 2);
        scheduler.scheduleWithFixedDelay(this::flushStaleBuffers, scanMs, scanMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 写入一条数据。达到批量立即刷出；全局缓冲超限先刷出本线程缓冲，
     * 仍超限则抛 {@link WriteRejectedException}。
     */
    public void write(T item) {
        if (closed.get()) {
            throw new IllegalStateException("batcher is closed");
        }
        ThreadBuffer b = local.get();
        b.lock.lock();
        try {
            if (globalBuffered.get() >= config.maxBufferedItems()) {
                flushLocked(b);
                if (globalBuffered.get() >= config.maxBufferedItems()) {
                    rejectedCount.incrementAndGet();
                    throw new WriteRejectedException(
                            "global buffer limit " + config.maxBufferedItems() + " reached");
                }
            }
            if (b.items.isEmpty()) {
                b.oldestItemNanos = System.nanoTime();
            }
            b.items.add(item);
            globalBuffered.incrementAndGet();
            if (b.items.size() >= config.maxBatchSize()) {
                flushLocked(b);
            }
        } finally {
            b.lock.unlock();
        }
    }

    /** 立即刷出当前线程的缓冲。 */
    public void flush() {
        ThreadBuffer b = local.get();
        b.lock.lock();
        try {
            flushLocked(b);
        } finally {
            b.lock.unlock();
        }
    }

    /** 用配置默认超时刷空所有线程的缓冲。 */
    public FlushReport flushAll() {
        return flushAll(config.flushAllTimeout());
    }

    /**
     * 刷空所有已注册线程的缓冲；超时后返回报告，unfinishedOwners 列出未完成线程。
     */
    public FlushReport flushAll(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            List<String> flushed = new ArrayList<>();
            List<String> unfinished = new ArrayList<>();
            for (ThreadBuffer b : buffers.values()) {
                b.lock.lock();
                try {
                    boolean ok = flushLocked(b);
                    (ok && b.items.isEmpty() ? flushed : unfinished).add(b.label());
                } finally {
                    b.lock.unlock();
                }
            }
            if (unfinished.isEmpty()) {
                return new FlushReport(true, List.copyOf(flushed), List.of());
            }
            if (System.nanoTime() >= deadline) {
                return new FlushReport(false, List.copyOf(flushed), List.copyOf(unfinished));
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
        }
    }

    /** 包裹任务：任务结束后刷出当前线程缓冲，用于线程池场景防止线程复用/回收丢数据。 */
    public Runnable wrap(Runnable task) {
        return () -> {
            try {
                task.run();
            } finally {
                flush();
            }
        };
    }

    /** 线程工厂：线程退出前自动刷出其缓冲。 */
    public AutoFlushingThreadFactory threadFactory() {
        return new AutoFlushingThreadFactory(this);
    }

    public BatcherStats stats() {
        Map<String, Integer> depths = new LinkedHashMap<>();
        buffers.values().stream()
                .sorted(java.util.Comparator.comparingLong(b -> b.id))
                .forEach(b -> {
                    b.lock.lock();
                    try {
                        depths.put(b.label(), b.items.size());
                    } finally {
                        b.lock.unlock();
                    }
                });
        long flushes = flushCount.get();
        long items = itemsFlushed.get();
        return new BatcherStats(
                flushes,
                items,
                flushes == 0 ? 0.0 : (double) items / flushes,
                rejectedCount.get(),
                flushFailures.get(),
                globalBuffered.get(),
                Map.copyOf(depths));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            scheduler.shutdownNow();
            flushAll(config.flushAllTimeout());
        }
    }

    private void flushStaleBuffers() {
        if (closed.get()) {
            return;
        }
        try {
            long now = System.nanoTime();
            long maxAge = config.flushInterval().toNanos();
            for (ThreadBuffer b : buffers.values()) {
                b.lock.lock();
                try {
                    if (!b.items.isEmpty() && now - b.oldestItemNanos >= maxAge) {
                        flushLocked(b);
                    }
                } finally {
                    b.lock.unlock();
                }
            }
        } catch (Throwable ignored) {
            // 后台线程绝不能因单次扫描异常而退出
        }
    }

    /**
     * 刷出单个缓冲（调用方必须持有 b.lock）。
     * 成功：清空缓冲、序号前进；失败：数据保留、序号不变，重试重发相同序号。
     */
    private boolean flushLocked(ThreadBuffer b) {
        if (b.items.isEmpty()) {
            return true;
        }
        Batch<T> batch = new Batch<>(b.id, b.ownerName, b.nextSequence, b.items);
        try {
            sink.write(batch);
        } catch (Exception e) {
            flushFailures.incrementAndGet();
            return false;
        }
        b.nextSequence++;
        int n = b.items.size();
        b.items.clear();
        globalBuffered.addAndGet(-n);
        flushCount.incrementAndGet();
        itemsFlushed.addAndGet(n);
        return true;
    }

    private final class ThreadBuffer {
        final long id;
        final String ownerName;
        final ReentrantLock lock = new ReentrantLock();
        final List<T> items = new ArrayList<>();
        long nextSequence;
        long oldestItemNanos;

        ThreadBuffer(long id, String ownerName) {
            this.id = id;
            this.ownerName = ownerName;
        }

        String label() {
            return ownerName + "#" + id;
        }
    }
}
