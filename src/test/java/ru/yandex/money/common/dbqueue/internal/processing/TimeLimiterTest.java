package ru.yandex.money.common.dbqueue.internal.processing;

import org.junit.Assert;
import org.junit.Test;
import ru.yandex.money.common.dbqueue.stub.FakeMillisTimeProvider;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Oleg Kandaurov
 * @since 18.10.2019
 */
public class TimeLimiterTest {

    @Test
    public void should_not_execute_when_timeout_is_zero() {
        TimeLimiter timeLimiter = new TimeLimiter(new FakeMillisTimeProvider(Collections.emptyList()), Duration.ZERO);
        timeLimiter.execute(ignored -> Assert.fail("should not invoke when duration is zero"));
    }

    @Test
    public void should_drain_timeout_to_zero() {
        Duration timeout = Duration.ofMillis(10);
        AtomicInteger executionCount = new AtomicInteger(0);
        TimeLimiter timeLimiter = new TimeLimiter(new FakeMillisTimeProvider(Arrays.asList(0L, 3L, 3L, 11L)), timeout);
        timeLimiter.execute(remainingTimeout -> {
            executionCount.incrementAndGet();
            Assert.assertEquals(timeout, remainingTimeout);
        });
        timeLimiter.execute(remainingTimeout -> {
            executionCount.incrementAndGet();
            Assert.assertEquals(Duration.ofMillis(7), remainingTimeout);
        });
        timeLimiter.execute(ignored -> Assert.fail("should not invoke when duration is zero"));
        Assert.assertEquals(2, executionCount.get());
    }
}