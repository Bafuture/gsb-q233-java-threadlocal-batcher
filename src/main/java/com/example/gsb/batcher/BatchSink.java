package com.example.gsb.batcher;

/**
 * 下游接收端。实现方必须保证幂等：同一 {@link Batch#idempotencyKey()}
 * 被重复投递时效果与只投递一次一致（可用 {@link DeduplicatingSink} 包装实现）。
 *
 * write 抛异常视为本次刷出失败，批写器会保留数据并用相同序号重试。
 */
@FunctionalInterface
public interface BatchSink<T> {
    void write(Batch<T> batch) throws Exception;
}
