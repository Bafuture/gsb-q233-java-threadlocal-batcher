package com.example.gsb.batcher;

import java.util.List;

/**
 * flushAll 结果。
 *
 * @param complete          是否在超时内把所有缓冲刷空
 * @param flushedOwners     本次已刷空的线程缓冲标签
 * @param unfinishedOwners  超时后仍未刷空的线程缓冲标签（如下游持续失败或写入方持续写入）
 */
public record FlushReport(boolean complete, List<String> flushedOwners, List<String> unfinishedOwners) {
}
