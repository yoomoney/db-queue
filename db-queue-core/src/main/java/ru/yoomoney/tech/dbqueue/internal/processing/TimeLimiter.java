package ru.yoomoney.tech.dbqueue.internal.processing;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Класс, для ограничения времени нескольких действий в заданный таймаут
 *
 * @author Oleg Kandaurov
 * @since 18.10.2019
 */
public class TimeLimiter {
    @Nonnull
    private final MillisTimeProvider millisTimeProvider;
    private Duration remainingTimeout;
    private Duration elapsedTime = Duration.ZERO;

    public TimeLimiter(@Nonnull MillisTimeProvider millisTimeProvider,
                       @Nonnull Duration timeout) {
        this.millisTimeProvider = Objects.requireNonNull(millisTimeProvider);
        this.remainingTimeout = Objects.requireNonNull(timeout);
    }

    /**
     * Выполнить действие с контролем времени.
     * Если заданный таймаут истёк, то действие не будет выполняться.
     *
     * @param consumer вызываемое действие, с передачей в аргументы оставшегося времени выполнения
     */
    public void execute(@Nonnull Consumer<Duration> consumer) {
        Objects.requireNonNull(consumer);
        if (remainingTimeout.equals(Duration.ZERO)) {
            return;
        }
        long startTime = millisTimeProvider.getMillis();
        consumer.accept(remainingTimeout);
        elapsedTime = elapsedTime.plus(Duration.ofMillis(millisTimeProvider.getMillis() - startTime));
        if (remainingTimeout.compareTo(elapsedTime) <= 0) {
            remainingTimeout = Duration.ZERO;
        } else {
            remainingTimeout = remainingTimeout.minus(elapsedTime);
        }
    }
}
