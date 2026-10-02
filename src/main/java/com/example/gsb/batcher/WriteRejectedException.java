package com.example.gsb.batcher;

/** 缓冲达到全局内存上限且策略为 REJECT 时抛出。 */
public class WriteRejectedException extends RuntimeException {
    public WriteRejectedException(String message) {
        super(message);
    }
}
