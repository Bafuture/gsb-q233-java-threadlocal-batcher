package com.example.gsb.batcher;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 线程本地缓冲与批量合并组件。
 *
 * <h2>顺序保证</h2>
 * <ul>
 *   <li>每个线程拥有独立缓冲，同一线程的写入严格按提交顺序出现在批次内、批次间，
 *       批次序号 (sequence) 从 0 开始单调递增。</li>
 *   <li>不同线程的刷出可能并发发生，其相对顺序<b>不做保证</b>；
 *       下游不得假设跨线程的全局顺序，只能假设单线程内有序。</li>
 * </ul>
 *
 * <h2>刷出时机</h2>
 * 达到批量大小、缓冲中最老数据超过 flushInterval、flushAll、线程退出（reaper 检测）
 * 或 batcher 关闭。
 *
 * <h2>内存上限</h2>
 * 所有线程缓冲条数之和不得超过 maxGlobalBuffered：超出时按 {@link OverflowPolicy}
 * 刷出当前线程缓冲或拒绝写入。
 *
 * <h2>崩溃安全</h2>
 * 每个批次带 (线程 id, 序号) 幂等键；下游可用 {@link IdempotentSink} 去重。
 * sink 抛异常时批次原样放回缓冲且序号不前进，重试仍带相同序号（at-least-once）。
 */
public final class ThreadLocalBatcher<T> implements AutoCloseable {

    private final BatchSink<T> sink;
    private final int batchSize;
    private final long flushIntervalNanos;
    private final int maxGlobalBuffered;
    private final OverflowPolicy overflowPolicy;
    private final Duration flushAllTimeout;

    private final Map<Thread, ThreadBuffer> buffers = new ConcurrentHashMap<>();
    private final ThreadLocal<ThreadBuffer> local = ThreadLocal.withInitial(this::registerBuffer);
    private final AtomicInteger globalBuffered = new AtomicInteger();

    private final AtomicLong flushCount = new AtomicLong();
    private final AtomicLong itemsWritten = new AtomicLong();
    private final AtomicLong itemsFlushed = new AtomicLong();
    private final AtomicLong rejectedCount = new AtomicLong();

    private final ScheduledExecutorService housekeeper;
    private volatile boolean closed;

    private ThreadLocalBatcher(Builder<T> b) {
        this.sink = b.sink;
        this.batchSize = b.batchSize;
        this.flushIntervalNanos = b.flushInterval.toNanos();
        this.maxGlobalBuffered = b.maxGlobalBuffered;
        this.overflowPolicy = b.overflowPolicy;
        this.flushAllTimeout = b.flushAllTimeout;

        long periodMillis = Math.max(1, Math.min(50, b.flushInterval.toMillis() / 2));
        this.housekeeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "batcher-housekeeper");
            t.setDaemon(true);
            return t;
        });
        this.housekeeper.scheduleAtFixedRate(this::housekeeping,
                periodMillis, periodMillis, TimeUnit.MILLISECONDS);
    }

    /** 写入一条数据到当前线程的缓冲。 */
    public void write(T item) {
        if (closed) {
            throw new IllegalStateException("batcher is closed");
        }
        ThreadBuffer buf = local.get();
        buf.lock.lock();
        try {
            if (globalBuffered.get() >= maxGlobalBuffered) {
                if (overflowPolicy == OverflowPolicy.REJECT) {
                    rejectedCount.incrementAndGet();
                    throw new WriteRejectedException(
                            "global buffered size reached " + maxGlobalBuffered);
                }
                flushLocked(buf);
            }
            if (buf.items.isEmpty()) {
                buf.oldestItemNanos = System.nanoTime();
            }
            buf.items.add(item);
            globalBuffered.incrementAndGet();
            itemsWritten.incrementAndGet();
            if (buf.items.size() >= batchSize) {
                flushLocked(buf);
            }
        } finally {
            buf.lock.unlock();
        }
    }

    /** 立即刷出当前线程的缓冲。 */
    public void flush() {
        ThreadBuffer buf = local.get();
        buf.lock.lock();
        try {
            flushLocked(buf);
        } finally {
            buf.lock.unlock();
        }
    }

    /**
     * 刷出所有线程的缓冲。逐个加锁并遵守截止时间；超时后把未刷空的线程
     * 列入 {@link FlushAllResult#pendingThreads()}。
     */
    public FlushAllResult flushAll(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        List<FlushAllResult.ThreadRef> flushed = new ArrayList<>();
        List<FlushAllResult.ThreadRef> pending = new ArrayList<>();

        for (ThreadBuffer buf : new ArrayList<>(buffers.values())) {
            long remainingNanos = deadline - System.nanoTime();
            boolean acquired = false;
            if (remainingNanos > 0) {
                try {
                    acquired = buf.lock.tryLock(remainingNanos, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (!acquired) {
                pending.add(buf.ref());
                continue;
            }
            try {
                if (!buf.items.isEmpty()) {
                    flushLocked(buf);
                }
                if (buf.items.isEmpty()) {
                    flushed.add(buf.ref());
                } else {
                    pending.add(buf.ref());
                }
            } finally {
                buf.lock.unlock();
            }
        }
        return new FlushAllResult(pending.isEmpty(), flushed, pending);
    }

    public FlushAllResult flushAll() {
        return flushAll(flushAllTimeout);
    }

    /** 统计快照。 */
    public BatcherStats stats() {
        long flushes = flushCount.get();
        long flushedItems = itemsFlushed.get();
        Map<String, Integer> depths = new LinkedHashMap<>();
        for (ThreadBuffer buf : buffers.values()) {
            buf.lock.lock();
            try {
                if (!buf.items.isEmpty()) {
                    depths.put(buf.ownerName, buf.items.size());
                }
            } finally {
                buf.lock.unlock();
            }
        }
        return new BatcherStats(flushes, itemsWritten.get(), flushedItems,
                flushes == 0 ? 0.0 : flushedItems / (double) flushes,
                rejectedCount.get(), Map.copyOf(depths));
    }

    /** 刷出所有缓冲（使用默认超时）并停止后台线程。 */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        flushAll(flushAllTimeout);
        housekeeper.shutdownNow();
    }

    // ---- internals ----

    private ThreadBuffer registerBuffer() {
        ThreadBuffer buf = new ThreadBuffer(Thread.currentThread());
        buffers.put(Thread.currentThread(), buf);
        return buf;
    }

    private void flushLocked(ThreadBuffer buf) {
        if (buf.items.isEmpty()) {
            return;
        }
        long seq = buf.nextSeq;
        FlushBatch<T> batch = new FlushBatch<>(
                buf.ownerId, buf.ownerName, seq, new ArrayList<>(buf.items), Instant.now());
        buf.items.clear();
        globalBuffered.addAndGet(-batch.size());
        try {
            sink.flush(batch);
        } catch (RuntimeException e) {
            // at-least-once：原样放回缓冲，序号不前进，重试仍是同一个幂等键
            buf.items.addAll(batch.items());
            buf.oldestItemNanos = System.nanoTime();
            globalBuffered.addAndGet(batch.size());
            throw new FlushException("flush failed for " + buf.ref(), e);
        }
        buf.nextSeq++;
        flushCount.incrementAndGet();
        itemsFlushed.addAndGet(batch.size());
    }

    private void housekeeping() {
        for (ThreadBuffer buf : buffers.values()) {
            if (closed) {
                return;
            }
            if (!buf.owner.isAlive()) {
                reapDeadThread(buf);
                continue;
            }
            boolean acquired = buf.lock.tryLock();
            if (!acquired) {
                continue;
            }
            try {
                if (!buf.items.isEmpty()
                        && System.nanoTime() - buf.oldestItemNanos >= flushIntervalNanos) {
                    flushLocked(buf);
                }
            } catch (RuntimeException ignored) {
                // 统计/日志由 sink 负责；下轮再试
            } finally {
                buf.lock.unlock();
            }
        }
    }

    private void reapDeadThread(ThreadBuffer buf) {
        if (!buf.lock.tryLock()) {
            return;
        }
        try {
            if (!buf.items.isEmpty()) {
                flushLocked(buf);
            }
            buffers.remove(buf.owner, buf);
        } catch (RuntimeException ignored) {
            // 线程已死无人重试，下轮再刷
        } finally {
            buf.lock.unlock();
        }
    }

    private final class ThreadBuffer {
        final Thread owner;
        final long ownerId;
        final String ownerName;
        final FlushAllResult.ThreadRef ref;
        final ReentrantLock lock = new ReentrantLock();
        final ArrayList<T> items = new ArrayList<>();
        long nextSeq;
        long oldestItemNanos;

        ThreadBuffer(Thread owner) {
            this.owner = owner;
            this.ownerId = owner.getId();
            this.ownerName = owner.getName();
            this.ref = new FlushAllResult.ThreadRef(ownerId, ownerName);
        }

        FlushAllResult.ThreadRef ref() {
            return ref;
        }
    }

    /** Builder。 */
    public static <T> Builder<T> builder(BatchSink<T> sink) {
        return new Builder<>(sink);
    }

    public static final class Builder<T> {
        private final BatchSink<T> sink;
        private int batchSize = 100;
        private Duration flushInterval = Duration.ofSeconds(1);
        private int maxGlobalBuffered = 10_000;
        private OverflowPolicy overflowPolicy = OverflowPolicy.FLUSH;
        private Duration flushAllTimeout = Duration.ofSeconds(10);

        private Builder(BatchSink<T> sink) {
            if (sink == null) {
                throw new IllegalArgumentException("sink must not be null");
            }
            this.sink = sink;
        }

        public Builder<T> batchSize(int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be positive");
            }
            this.batchSize = batchSize;
            return this;
        }

        public Builder<T> flushInterval(Duration flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder<T> maxGlobalBuffered(int maxGlobalBuffered) {
            if (maxGlobalBuffered <= 0) {
                throw new IllegalArgumentException("maxGlobalBuffered must be positive");
            }
            this.maxGlobalBuffered = maxGlobalBuffered;
            return this;
        }

        public Builder<T> overflowPolicy(OverflowPolicy policy) {
            this.overflowPolicy = policy;
            return this;
        }

        public Builder<T> flushAllTimeout(Duration timeout) {
            this.flushAllTimeout = timeout;
            return this;
        }

        public ThreadLocalBatcher<T> build() {
            return new ThreadLocalBatcher<>(this);
        }
    }
}
