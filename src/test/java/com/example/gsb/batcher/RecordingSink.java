package com.example.gsb.batcher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** 测试用：记录所有收到的批次，可选在刷出时阻塞。 */
final class RecordingSink<T> implements BatchSink<T> {

    final List<FlushBatch<T>> batches = new CopyOnWriteArrayList<>();
    volatile long sleepMillis = 0;

    @Override
    public void flush(FlushBatch<T> batch) {
        if (sleepMillis > 0) {
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        batches.add(batch);
    }

    List<T> allItems() {
        List<T> out = new ArrayList<>();
        for (FlushBatch<T> b : batches) {
            out.addAll(b.items());
        }
        return out;
    }
}
