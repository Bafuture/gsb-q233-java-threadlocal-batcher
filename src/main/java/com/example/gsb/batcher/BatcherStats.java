package com.example.gsb.batcher;

import java.util.Map;

/**
 * 统计快照。
 *
 * @param flushCount     成功刷出次数
 * @param itemsFlushed   累计刷出条目数
 * @param mergeRatio     合并比例 = itemsFlushed / flushCount（平均每次刷出合并的条目数，无刷出时为 0）
 * @param rejectedCount  因内存上限被拒绝的写入次数
 * @param flushFailures  刷出失败（下游抛异常）次数，失败后数据保留并会用相同序号重试
 * @param globalBuffered 当前全局缓冲条目数
 * @param bufferDepths   各线程缓冲深度，key 为 "线程名#缓冲id"
 */
public record BatcherStats(
        long flushCount,
        long itemsFlushed,
        double mergeRatio,
        long rejectedCount,
        long flushFailures,
        long globalBuffered,
        Map<String, Integer> bufferDepths) {
}
