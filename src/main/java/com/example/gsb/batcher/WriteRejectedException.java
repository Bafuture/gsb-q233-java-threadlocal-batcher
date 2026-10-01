package com.example.gsb.batcher;

/** 缓冲总大小达到上限且无法通过刷出腾出空间时抛出。 */
public final class WriteRejectedException extends RuntimeException {
    public WriteRejectedException(String message) {
        super(message);
    }
}
