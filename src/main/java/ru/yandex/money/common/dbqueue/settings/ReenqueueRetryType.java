package ru.yandex.money.common.dbqueue.settings;

import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskRecord;

import java.time.Duration;

/**
 * Type of the strategy, which computes the delay before
 * the next task execution if the task has to be brought back
 * {@link TaskExecutionResult.Type#REENQUEUE to the queue}.
 *
 * @author Dmitry Komarov
 * @since 21.05.2019
 */
public enum ReenqueueRetryType {

    /**
     * The task is deferred by the delay set manually with method
     * {@link TaskExecutionResult#reenqueue(Duration)} call.
     * <p>
     * Default value for the task postponing strategy.
     * <p>
     * Settings example:
     * <pre>
     * {@code db-queue.queueName.reenqueue-retry-type=manual}
     * </pre>
     */
    MANUAL,

    /**
     * The task is deferred by the delay set with the sequence of delays.
     * Delay is selected from the sequence according
     * to {@link TaskRecord#getReenqueueAttemptsCount() the number of task processing attempt}.
     * If the attempt number is bigger than the index of the last item in the sequence,
     * then the last item will be used.
     * <p>
     * For example, let the following sequence is set out in the settings:
     * <pre>
     * {@code
     * db-queue.queueName.reenqueue-retry-type=sequential
     * db-queue.queueName.reenqueue-retry-plan=PT1S,PT10S,PT1M,P7D}
     * </pre>
     * For the first attempt to defer the task a delay of 1 second will be chosen ({@code PT1S}),
     * for the second one it will be 10 seconds and so forth.
     * For the fifth attempt and all the next after the delay will be 7 days.
     */
    SEQUENTIAL,

    /**
     * The task is deferred by the fixed delay, which is set in configuration.
     * <p>
     * Settings example:
     * <pre>
     * {@code
     * db-queue.queueName.reenqueue-retry-type=fixed
     * db-queue.queueName.reenqueue-retry-delay=PT10S}
     * </pre>
     * Means that for each attempt the task will be deferred for 10 seconds.
     */
    FIXED,

    /**
     * The task is deferred by the delay set using an arithmetic progression.
     * The term of progression selected according
     * to {@link TaskRecord#getReenqueueAttemptsCount() the number of attempt to postpone the task processing}.
     * <p>
     * The progression is set by a pair of values: the initial term ({@code reenqueue-retry-initial-delay})
     * and the difference ({@code reenqueue-retry-step}).
     * <p>
     * Settings example:
     * <pre>
     * {@code
     * db-queue.queueName.reenqueue-retry-type=arithmetic
     * db-queue.queueName.reenqueue-retry-initial-delay=PT1S
     * db-queue.queueName.reenqueue-retry-step=PT2S}
     * </pre>
     * Means that the task will be deferred with following delays: {@code 1 second, 3 seconds, 5 seconds, 7 seconds, ...}
     */
    ARITHMETIC,

    /**
     * The task is deferred by the delay set using a geometric progression
     * The term of progression selected according to
     * {@link TaskRecord#getReenqueueAttemptsCount() the number of attempt to postpone the task processing}.
     * <p>
     * The progression is set by a pair of values: the initial term and the integer denominator.
     * <p>
     * Settings example:
     * <pre>
     * {@code
     * db-queue.queueName.reenqueue-retry-type=geometric
     * db-queue.queueName.reenqueue-retry-initial-delay=PT1S
     * db-queue.queueName.reenqueue-retry-ratio=2}
     * </pre>
     * Means that the task will be deferred with following delays: {@code 1 second, 2 seconds, 4 seconds, 8 seconds, ...}
     */
    GEOMETRIC
}
