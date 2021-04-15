package com.tencent.tdmq.handlers.rocketmq.inner.exception;

public class RopEncodeException extends Exception {

    public RopEncodeException() {
        super();
    }

    public RopEncodeException(String message) {
        super(message);
    }

    public RopEncodeException(String message, Throwable cause) {
        super(message, cause);
    }

    public RopEncodeException(Throwable cause) {
        super(cause);
    }
}
