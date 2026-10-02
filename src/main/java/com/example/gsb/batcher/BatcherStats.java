package com.example.gsb.batcher;

import java.util.Map;

/**
 * 统计快照。
 *
 * @param flushCount    累计刷出次数
 * @param itemsWritten  累计写入条数
 * @param itemsFlushed  累计刷出条数
 * @param mergeRatio    合并比例 = 刷出条数 / 刷出次数（平均每次刷出合并了多少条）
 * @param rejectedCount 因内存上限被拒绝的写入次数
 * @param bufferDepths  各线程当前缓冲深度（线程名 -> 条数）
 */
public record BatcherStats(
        long flushCount,
        long itemsWritten,
        long itemsFlushed,
        double mergeRatio,
        long rejectedCount,
        Map<String, Integer> bufferDepths) {
}
