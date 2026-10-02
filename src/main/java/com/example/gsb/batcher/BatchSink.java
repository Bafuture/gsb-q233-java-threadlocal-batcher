package com.example.gsb.batcher;

/**
 * 下游刷出目标。实现必须线程安全：不同线程的缓冲可能并发刷出。
 * 同一线程的批次严格按序号顺序到达；不同线程的批次相对顺序不做保证。
 *
 * 注意：flush 在持有该线程缓冲锁的情况下被调用，实现不得回调
 * 同一个 batcher 的 write/flush，否则会死锁。
 */
@FunctionalInterface
public interface BatchSink<T> {
    void flush(FlushBatch<T> batch);
}
