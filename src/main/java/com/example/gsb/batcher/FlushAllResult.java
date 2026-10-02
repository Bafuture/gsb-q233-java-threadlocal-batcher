package com.example.gsb.batcher;

import java.util.List;

/**
 * flushAll 的结果。complete 为 true 表示所有线程缓冲均已刷空；
 * 超时或失败时 pendingThreads 列出未完成刷出的线程。
 */
public record FlushAllResult(
        boolean complete,
        List<ThreadRef> flushedThreads,
        List<ThreadRef> pendingThreads) {

    public boolean timedOut() {
        return !complete;
    }

    /** 线程标识快照。 */
    public record ThreadRef(long threadId, String threadName) {
        @Override
        public String toString() {
            return threadName + "(" + threadId + ")";
        }
    }
}
