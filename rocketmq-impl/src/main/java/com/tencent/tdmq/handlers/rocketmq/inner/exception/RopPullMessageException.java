package com.tencent.tdmq.handlers.rocketmq.inner.exception;

public class RopPullMessageException extends Exception{

    public RopPullMessageException() {
        super();
    }

    public RopPullMessageException(String message) {
        super(message);
    }

    public RopPullMessageException(String message, Throwable cause) {
        super(message, cause);
    }

    public RopPullMessageException(Throwable cause) {
        super(cause);
    }
}
