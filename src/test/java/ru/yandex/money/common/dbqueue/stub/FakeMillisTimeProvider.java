package ru.yandex.money.common.dbqueue.stub;

import ru.yandex.money.common.dbqueue.internal.MillisTimeProvider;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class FakeMillisTimeProvider implements MillisTimeProvider {

    private final long firstTime;
    private final long secondTime;
    private int invocationCount;

    public FakeMillisTimeProvider(long firstTime, long secondTime) {
        this.firstTime = firstTime;
        this.secondTime = secondTime;
    }

    @Override
    public long getMillis() {
        invocationCount++;
        if (invocationCount == 1) {
            return firstTime;
        } else if (invocationCount == 2) {
            return secondTime;
        }
        throw new IllegalStateException("no more than two invocations");
    }
}
