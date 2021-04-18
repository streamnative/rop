package com.tencent.tdmq.handlers.rocketmq.inner.timer;

import java.util.concurrent.TimeUnit;

public class SystemTime implements Time {

    public SystemTime() {
    }

    public long milliseconds() {
        return System.currentTimeMillis();
    }

    public long hiResClockMs() {
        return TimeUnit.NANOSECONDS.toMillis(this.nanoseconds());
    }

    public long nanoseconds() {
        return System.nanoTime();
    }

    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

    }
}