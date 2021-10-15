package ru.yoomoney.tech.dbqueue.internal.processing;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueRetryType;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueSettings;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ReenqueueRetryStrategyTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void should_throw_exception_when_calculate_delay_with_manual_strategy() {
        ReenqueueSettings settings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.MANUAL).build();

        ReenqueueRetryStrategy strategy = ReenqueueRetryStrategy.Factory.create(settings);

        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("re-enqueue delay must be set explicitly via 'reenqueue(Duration)' method call");

        strategy.calculateDelay(createTaskRecord(0));
    }

    @Test
    public void should_calculate_delay_when_using_fixed_delay_strategy() {
        Duration fixedDelay = Duration.ofSeconds(10L);
        ReenqueueSettings settings = ReenqueueSettings.builder()
                .withRetryType(ReenqueueRetryType.FIXED)
                .withFixedDelay(fixedDelay)
                .build();

        ReenqueueRetryStrategy strategy = ReenqueueRetryStrategy.Factory.create(settings);

        List<Duration> delays = IntStream.range(0, 5)
                .mapToObj(ReenqueueRetryStrategyTest::createTaskRecord)
                .map(strategy::calculateDelay)
                .collect(Collectors.toList());
        assertThat(delays, equalTo(Arrays.asList(fixedDelay, fixedDelay, fixedDelay, fixedDelay, fixedDelay)));
    }

    @Test
    public void should_calculate_delay_when_using_sequential_strategy() {
        ReenqueueSettings settings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.SEQUENTIAL)
                .withSequentialPlan(Arrays.asList(Duration.ofSeconds(1L), Duration.ofSeconds(2L), Duration.ofSeconds(3L)))
                .build();

        ReenqueueRetryStrategy strategy = ReenqueueRetryStrategy.Factory.create(settings);

        List<Duration> delays = IntStream.range(0, 5)
                .mapToObj(ReenqueueRetryStrategyTest::createTaskRecord)
                .map(strategy::calculateDelay)
                .collect(Collectors.toList());
        assertThat(delays, equalTo(Arrays.asList(
                Duration.ofSeconds(1L),
                Duration.ofSeconds(2L),
                Duration.ofSeconds(3L),
                Duration.ofSeconds(3L),
                Duration.ofSeconds(3L)
        )));
    }

    @Test
    public void should_calculate_delay_when_using_arithmetic_strategy() {
        ReenqueueSettings settings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.ARITHMETIC)
                .withInitialDelay(Duration.ofSeconds(10L))
                .withArithmeticStep(Duration.ofSeconds(1L))
                .build();

        ReenqueueRetryStrategy strategy = ReenqueueRetryStrategy.Factory.create(settings);

        List<Duration> delays = IntStream.range(0, 5)
                .mapToObj(ReenqueueRetryStrategyTest::createTaskRecord)
                .map(strategy::calculateDelay)
                .collect(Collectors.toList());
        assertThat(delays, equalTo(Arrays.asList(
                Duration.ofSeconds(10L),
                Duration.ofSeconds(11L),
                Duration.ofSeconds(12L),
                Duration.ofSeconds(13L),
                Duration.ofSeconds(14L)
        )));
    }

    @Test
    public void should_calculate_delay_when_using_geometric_strategy() {
        ReenqueueSettings settings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.GEOMETRIC)
                .withInitialDelay(Duration.ofSeconds(10L))
                .withGeometricRatio(3L)
                .build();

        ReenqueueRetryStrategy strategy = ReenqueueRetryStrategy.Factory.create(settings);

        List<Duration> delays = IntStream.range(0, 5)
                .mapToObj(ReenqueueRetryStrategyTest::createTaskRecord)
                .map(strategy::calculateDelay)
                .collect(Collectors.toList());
        assertThat(delays, equalTo(Arrays.asList(
                Duration.ofSeconds(10L),
                Duration.ofSeconds(30L),
                Duration.ofSeconds(90L),
                Duration.ofSeconds(270L),
                Duration.ofSeconds(810L)
        )));
    }

    @Nonnull
    private static TaskRecord createTaskRecord(long reenqueueAttemptsCount) {
        return TaskRecord.builder()
                .withReenqueueAttemptsCount(reenqueueAttemptsCount)
                .withTotalAttemptsCount(reenqueueAttemptsCount).build();
    }
}
