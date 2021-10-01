package ru.yoomoney.tech.dbqueue.settings;

/**
 * Strategy type for the task deferring in case of retry.
 *
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public enum FailRetryType {

    /**
     * The task is deferred exponentially relative to the interval
     * {@link FailureSettings#getRetryInterval()}
     * The denominator of the progression equals 2.
     * First 6 terms: 1 2 4 8 16 32
     */
    GEOMETRIC_BACKOFF,
    /**
     * The task is deferred by an arithmetic progression relative to the interval
     * {@link FailureSettings#getRetryInterval()}.
     * The difference of progression equals 2.
     * First 6 terms: 1 3 5 7 9 11
     */
    ARITHMETIC_BACKOFF,
    /**
     * The task is deferred with fixed delay.
     * <p>
     * Fixed delay value is set through
     * {@link FailureSettings#getRetryInterval()}
     */
    LINEAR_BACKOFF
}
