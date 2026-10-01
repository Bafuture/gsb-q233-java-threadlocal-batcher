package com.example.gsb.batcher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;

final class TestSupport {

    static final class RecordingSink<T> implements BatchSink<T> {
        final List<Batch<T>> batches = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void write(Batch<T> batch) {
            batches.add(batch);
        }

        List<T> allItems() {
            List<T> out = new ArrayList<>();
            synchronized (batches) {
                for (Batch<T> b : batches) {
                    out.addAll(b.items());
                }
            }
            return out;
        }
    }

    static void awaitUntil(BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    private TestSupport() {
    }
}
