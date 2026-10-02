package com.example.gsb.batcher;

/** 全局缓冲达到内存上限时的策略。 */
public enum OverflowPolicy {
    /** 先刷出当前线程的缓冲腾挪空间，再接受写入（不丢数据，可能增加小批次）。 */
    FLUSH,
    /** 拒绝写入并抛出 {@link WriteRejectedException}，由调用方决定重试或降级。 */
    REJECT
}
