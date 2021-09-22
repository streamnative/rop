package org.streamnative.pulsar.handlers.rocketmq.inner.exception;

public class RopServerException extends Exception {

    public RopServerException() {
        super();
    }

    public RopServerException(String message) {
        super(message);
    }

    public RopServerException(String message, Throwable cause) {
        super(message, cause);
    }

    public RopServerException(Throwable cause) {
        super(cause);
    }

    protected RopServerException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
