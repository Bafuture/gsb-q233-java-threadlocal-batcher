package com.example.gsb.batcher;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 幂等包装器：按 (ownerId, sequence) 去重，重复批次直接丢弃，
 * 保证崩溃重试 / 重复刷出后下游结果与只刷一次一致。
 */
public final class DeduplicatingSink<T> implements BatchSink<T> {
    private final BatchSink<T> delegate;
    private final Set<String> seen = ConcurrentHashMap.newKeySet();
    private final AtomicLong duplicatesDropped = new AtomicLong();

    public DeduplicatingSink(BatchSink<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void write(Batch<T> batch) throws Exception {
        if (seen.add(batch.idempotencyKey())) {
            delegate.write(batch);
        } else {
            duplicatesDropped.incrementAndGet();
        }
    }

    public long duplicatesDropped() {
        return duplicatesDropped.get();
    }
}
