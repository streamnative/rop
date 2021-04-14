package com.tencent.tdmq.handlers.rocketmq.inner.exception;

public class RopSendException extends Exception{

    public RopSendException() {
        super();
    }

    public RopSendException(String message) {
        super(message);
    }

    public RopSendException(String message, Throwable cause) {
        super(message, cause);
    }

    public RopSendException(Throwable cause) {
        super(cause);
    }
}
