package com.example.gsb.batcher;

import java.util.HashSet;
import java.util.Set;

/**
 * 幂等包装器：按 (线程 id, 序号) 去重，重复刷出的批次只生效一次。
 * 用于崩溃重试场景——batcher 采用 at-least-once 语义，
 * 下游经本包装器去重后，结果与只刷一次一致。
 */
public final class IdempotentSink<T> implements BatchSink<T> {

    private final BatchSink<T> delegate;
    private final Set<String> seen = new HashSet<>();

    public IdempotentSink(BatchSink<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void flush(FlushBatch<T> batch) {
        synchronized (seen) {
            if (!seen.add(batch.idempotencyKey())) {
                return;
            }
        }
        delegate.flush(batch);
    }

    public int deduplicatedCount() {
        synchronized (seen) {
            return seen.size();
        }
    }
}
