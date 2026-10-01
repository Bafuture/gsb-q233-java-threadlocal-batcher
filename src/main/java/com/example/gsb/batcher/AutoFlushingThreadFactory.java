package com.example.gsb.batcher;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 线程工厂：所建线程在 run 结束后自动刷出其线程本地缓冲，
 * 保证线程退出（含线程池回收线程）不丢数据。
 */
public final class AutoFlushingThreadFactory implements ThreadFactory {
    private final ThreadLocalBatcher<?> batcher;
    private final AtomicLong counter = new AtomicLong();

    AutoFlushingThreadFactory(ThreadLocalBatcher<?> batcher) {
        this.batcher = batcher;
    }

    @Override
    public Thread newThread(Runnable r) {
        Runnable wrapped = () -> {
            try {
                r.run();
            } finally {
                batcher.flush();
            }
        };
        Thread t = new Thread(wrapped, "batcher-worker-" + counter.getAndIncrement());
        t.setDaemon(true);
        return t;
    }
}
