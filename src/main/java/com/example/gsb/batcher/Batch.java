package com.example.gsb.batcher;

import java.util.List;

/**
 * 一次刷出的数据单元。携带 (ownerId, sequence) 作为幂等键：
 * 同一 owner 的序号单调递增，崩溃重试会重发相同序号，下游据此去重。
 *
 * 顺序保证：同一 owner（同一线程缓冲）内的 items 严格按提交顺序排列，
 * 且同一 owner 的批次按 sequence 递增刷出；不同 owner 之间的相对顺序不做保证。
 */
public final class Batch<T> {
    private final long ownerId;
    private final String ownerName;
    private final long sequence;
    private final List<T> items;

    public Batch(long ownerId, String ownerName, long sequence, List<T> items) {
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.sequence = sequence;
        this.items = List.copyOf(items);
    }

    public long ownerId() {
        return ownerId;
    }

    public String ownerName() {
        return ownerName;
    }

    public long sequence() {
        return sequence;
    }

    public List<T> items() {
        return items;
    }

    public int size() {
        return items.size();
    }

    /** 幂等键：下游按此键去重，重复投递同一键必须被忽略。 */
    public String idempotencyKey() {
        return ownerId + ":" + sequence;
    }

    @Override
    public String toString() {
        return "Batch{" + idempotencyKey() + ", owner=" + ownerName + ", items=" + items.size() + '}';
    }
}
