package com.example.gsb.batcher;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class IdempotencyTest {

    private static BatcherConfig.Builder deterministic() {
        return BatcherConfig.builder()
                .flushInterval(Duration.ofHours(1))
                .maxBufferedItems(1_000_000);
    }

    @Test
    void duplicateBatchDeliveryIsDeduplicatedBySequence() {
        TestSupport.RecordingSink<String> downstream = new TestSupport.RecordingSink<>();
        DeduplicatingSink<String> dedup = new DeduplicatingSink<>(downstream);
        try (ThreadLocalBatcher<String> batcher =
                new ThreadLocalBatcher<>(deterministic().maxBatchSize(2).build(), dedup)) {
            batcher.write("a");
            batcher.write("b"); // 触发刷出 batch(owner, seq=0)=[a,b]
            assertThat(downstream.allItems()).containsExactly("a", "b");

            // 模拟崩溃恢复后用同一个幂等键重复刷出
            Batch<String> first = downstream.batches.get(0);
            Batch<String> retry = new Batch<>(
                    first.ownerId(), first.ownerName(), first.sequence(),
                    new ArrayList<>(first.items()));
            try {
                dedup.write(retry);
                dedup.write(retry);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            // 下游结果与只刷一次一致
            assertThat(downstream.allItems()).containsExactly("a", "b");
            assertThat(dedup.duplicatesDropped()).isEqualTo(2);
        }
    }

    /**
     * 下游“已生效但响应失败”：批写器保留数据、用相同序号重试；
     * 去重包装器保证重复部分不会重复落地。
     */
    @Test
    void retryAfterFailureReusesSequenceAndLeavesSingleCopy() {
        List<String> applied = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean failOnce = new AtomicBoolean(true);

        BatchSink<String> flaky = batch -> {
            applied.addAll(batch.items()); // 先生效
            if (failOnce.get()) {
                throw new RuntimeException("simulated crash after apply: " + batch.sequence());
            }
        };
        DeduplicatingSink<String> dedup = new DeduplicatingSink<>(flaky);
        try (ThreadLocalBatcher<String> batcher =
                new ThreadLocalBatcher<>(deterministic().maxBatchSize(2).build(), dedup)) {
            batcher.write("x");
            batcher.write("y"); // 第一次刷出失败，数据保留、序号不变
            assertThat(batcher.stats().flushFailures()).isEqualTo(1);
            assertThat(batcher.stats().flushCount()).isZero();

            failOnce.set(false);
            batcher.flush(); // 重试，相同 (owner, seq)

            assertThat(batcher.stats().flushFailures()).isEqualTo(1);
            assertThat(batcher.stats().flushCount()).isEqualTo(1);
            // 重试重发了同序号批次，去重后落地结果与只成功一次一致
            synchronized (applied) {
                assertThat(applied).containsExactly("x", "y");
            }
            assertThat(dedup.duplicatesDropped()).isEqualTo(1);
        }
    }
}
