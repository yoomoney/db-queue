package ru.yandex.money.common.dbqueue.settings;

/**
 * Strategy type for the task deferring in case of retry.
 *
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public enum TaskRetryType {

    /**
     * The task is deferred exponentially relative to the interval
     * {@link QueueSettings#getRetryInterval()}
     * The denominator of the progression equals 2.
     * First 6 terms: 1 2 4 8 16 32
     */
    GEOMETRIC_BACKOFF,
    /**
     * The task is deferred by an arithmetic progression relative to the interval
     * {@link QueueSettings#getRetryInterval()}.
     * The difference of progression equals 2.
     * First 6 terms: 1 3 5 7 9 11
     */
    ARITHMETIC_BACKOFF,
    /**
     * The task is deferred with fixed delay.
     * <p>
     * Fixed delay value is set through
     * {@link QueueSettings#getRetryInterval()}
     */
    LINEAR_BACKOFF
}
