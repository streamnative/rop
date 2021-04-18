package com.tencent.tdmq.handlers.rocketmq.inner.timer;

public interface Time {
    Time SYSTEM = new SystemTime();

    long milliseconds();

    long hiResClockMs();

    long nanoseconds();

    void sleep(long var1);
}
