package com.example.gsb.batcher;

import java.time.Duration;
import java.util.Objects;

/** 批写器配置。 */
public final class BatcherConfig {
    private final int maxBatchSize;
    private final long maxBufferedItems;
    private final Duration flushInterval;
    private final Duration flushAllTimeout;

    private BatcherConfig(Builder b) {
        this.maxBatchSize = b.maxBatchSize;
        this.maxBufferedItems = b.maxBufferedItems;
        this.flushInterval = b.flushInterval;
        this.flushAllTimeout = b.flushAllTimeout;
    }

    /** 单线程缓冲达到该条数即触发刷出。 */
    public int maxBatchSize() {
        return maxBatchSize;
    }

    /** 全局（所有线程合计）缓冲条数上限；超过则先尝试刷出，仍放不下则拒绝写入。 */
    public long maxBufferedItems() {
        return maxBufferedItems;
    }

    /** 缓冲内最老条目的最长驻留时间，超时由后台线程刷出。 */
    public Duration flushInterval() {
        return flushInterval;
    }

    /** flushAll() 默认等待时长。 */
    public Duration flushAllTimeout() {
        return flushAllTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static BatcherConfig defaults() {
        return builder().build();
    }

    public static final class Builder {
        private int maxBatchSize = 100;
        private long maxBufferedItems = 10_000;
        private Duration flushInterval = Duration.ofSeconds(1);
        private Duration flushAllTimeout = Duration.ofSeconds(5);

        public Builder maxBatchSize(int v) {
            if (v < 1) throw new IllegalArgumentException("maxBatchSize must be >= 1");
            this.maxBatchSize = v;
            return this;
        }

        public Builder maxBufferedItems(long v) {
            if (v < 1) throw new IllegalArgumentException("maxBufferedItems must be >= 1");
            this.maxBufferedItems = v;
            return this;
        }

        public Builder flushInterval(Duration v) {
            this.flushInterval = Objects.requireNonNull(v);
            return this;
        }

        public Builder flushAllTimeout(Duration v) {
            this.flushAllTimeout = Objects.requireNonNull(v);
            return this;
        }

        public BatcherConfig build() {
            return new BatcherConfig(this);
        }
    }
}
