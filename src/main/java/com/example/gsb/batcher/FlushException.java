package com.example.gsb.batcher;

/** 下游 sink 刷出失败时抛出。失败的批次已放回缓冲，可安全重试（序号不变）。 */
public class FlushException extends RuntimeException {
    public FlushException(String message, Throwable cause) {
        super(message, cause);
    }
}
