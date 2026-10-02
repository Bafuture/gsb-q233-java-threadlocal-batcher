package com.example.gsb.batcher;

import java.time.Instant;
import java.util.List;

/**
 * 一次刷出的批次。携带 (ownerThreadId, sequence) 作为幂等键：
 * 同一个线程的批次序号从 0 开始严格递增，下游按该键去重后，
 * 重复刷出与只刷一次的结果一致。
 */
public record FlushBatch<T>(
        long ownerThreadId,
        String ownerThreadName,
        long sequence,
        List<T> items,
        Instant createdAt) {

    public FlushBatch {
        items = List.copyOf(items);
    }

    /** 幂等键：线程 id + 序号。 */
    public String idempotencyKey() {
        return ownerThreadId + ":" + sequence;
    }

    public int size() {
        return items.size();
    }
}
