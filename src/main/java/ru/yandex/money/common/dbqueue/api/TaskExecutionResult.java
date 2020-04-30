package ru.yandex.money.common.dbqueue.api;


import ru.yandex.money.common.dbqueue.settings.ReenqueueRetryType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * The action, which should be performed after the task processing.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public final class TaskExecutionResult {

    /**
     * Action performed after task processing
     */
    public enum Type {
        /**
         * Postpone (re-enqueue) the task, so that the task will be executed again
         */
        REENQUEUE,
        /**
         * Finish the task, task will be removed from the queue
         */
        FINISH,
        /**
         * Notify on error task execution, task will be postponed and executed again
         */
        FAIL
    }

    private static final TaskExecutionResult FINISH = new TaskExecutionResult(Type.FINISH);
    private static final TaskExecutionResult FAIL = new TaskExecutionResult(Type.FAIL);
    private static final TaskExecutionResult REENQUEUE_WITHOUT_DELAY = new TaskExecutionResult(Type.REENQUEUE);

    @Nonnull
    private final Type actionType;
    @Nullable
    private final Duration executionDelay;

    private TaskExecutionResult(@Nonnull Type actionType, @Nullable Duration executionDelay) {
        this.actionType = Objects.requireNonNull(actionType);
        this.executionDelay = executionDelay;
    }

    private TaskExecutionResult(@Nonnull Type actionType) {
        this(actionType, null);
    }

    /**
     * Get action, which should be performed after the task processing.
     *
     * @return action, which should be performed after the task processing.
     */
    @Nonnull
    public Type getActionType() {
        return actionType;
    }

    /**
     * Get task execution delay.
     *
     * @return task execution delay.
     */
    @Nonnull
    public Optional<Duration> getExecutionDelay() {
        return Optional.ofNullable(executionDelay);
    }

    /**
     * Get task execution delay or throw an {@linkplain IllegalStateException}
     * when task execution delay is not present.
     *
     * @return task execution delay.
     * @throws IllegalStateException An exception when task execution delay is not present.
     */
    @Nonnull
    public Duration getExecutionDelayOrThrow() {
        if (executionDelay == null) {
            throw new IllegalArgumentException("executionDelay is absent");
        }
        return executionDelay;
    }

    /**
     * Instruction to re-enqueue the task with determined execution delay.
     * <br>
     * Re-enqueue attempts counter will be reset, task will be executed again after the given execution delay.
     *
     * @param delay determined execution delay, after which the task will be executed again.
     * @return Task execution action.
     */
    @Nonnull
    public static TaskExecutionResult reenqueue(@Nonnull Duration delay) {
        Objects.requireNonNull(delay);
        return new TaskExecutionResult(Type.REENQUEUE, delay);
    }

    /**
     * Instruction to re-enqueue the task using the {@link ReenqueueRetryType re-enqueue strategy}
     * established in the queue configuration.
     * Re-enqueue attempts counter will be reset.
     *
     * @return Task execution action.
     */
    @Nonnull
    public static TaskExecutionResult reenqueue() {
        return REENQUEUE_WITHOUT_DELAY;
    }

    /**
     * Instruction to execute the task again later according to the {@link ReenqueueRetryType re-enqueue strategy}
     * established in the queue configuration.
     *
     * @return Task execution action.
     */
    @Nonnull
    public static TaskExecutionResult fail() {
        return FAIL;
    }

    /**
     * Instruction to finish task processing and remove the task from the queue
     *
     * @return Task execution action.
     */
    @Nonnull
    public static TaskExecutionResult finish() {
        return FINISH;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TaskExecutionResult that = (TaskExecutionResult) obj;
        return actionType == that.actionType &&
                Objects.equals(executionDelay, that.executionDelay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actionType, executionDelay);
    }

    @Override
    public String toString() {
        return '{' +
                "actionType=" + actionType +
                (executionDelay == null ? "" : ", executionDelay=" + executionDelay) +
                '}';
    }
}
