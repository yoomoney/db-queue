package ru.yandex.money.common.dbqueue.api;


import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Описывает действие, которое следует предпринять после обработки очередной задачи.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public final class TaskExecutionResult {

    /**
     * Действие, выполняемое после обработки задачи
     */
    public enum Type {
        /**
         * Переставить выполнение задачи
         */
        REENQUEUE,
        /**
         * Завершить задачу
         */
        FINISH,
        /**
         * Сигнализировать о неуспешном выполнении задачи
         */
        FAIL
    }

    private static final TaskExecutionResult FINISH = new TaskExecutionResult(Type.FINISH);
    private static final TaskExecutionResult FAIL = new TaskExecutionResult(Type.FAIL);

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
     * Получить действие, выполняемое после обработки задачи
     *
     * @return действие после обработки
     */
    @Nonnull
    public Type getActionType() {
        return actionType;
    }

    /**
     * Получить время задержки обработки.
     *
     * @return время задержки обработки
     */
    @Nonnull
    public Optional<Duration> getExecutionDelay() {
        return Optional.ofNullable(executionDelay);
    }

    /**
     * Получить время задержки обработки или выбросить исключение.
     *
     * @return время задержки обработки
     */
    @Nonnull
    public Duration getExecutionDelayOrThrow() {
        if (executionDelay == null) {
            throw new IllegalArgumentException("executionDelay is absent");
        }
        return executionDelay;
    }

    /**
     * Указание поставить задачу в очередь повторно, с указанием фиксированного значения задержки.
     * Попытки обработки сбрасываются, задача будет выполнена через указанный период.
     *
     * @param delay фиксированное значение задержки, через которое задача должна быть выполнена повторно
     * @return действие
     */
    @Nonnull
    public static TaskExecutionResult reenqueue(@Nonnull Duration delay) {
        Objects.requireNonNull(delay);
        return new TaskExecutionResult(Type.REENQUEUE, delay);
    }

    /**
     * Указание повторить задачу позже
     *
     * @return действие
     */
    @Nonnull
    public static TaskExecutionResult fail() {
        return FAIL;
    }

    /**
     * Указание повторить задачу позже через фиксированное время
     *
     * @param delay значение задержки
     * @return действие
     */
    @Nonnull
    public static TaskExecutionResult fail(@Nonnull Duration delay) {
        Objects.requireNonNull(delay);
        return new TaskExecutionResult(Type.FAIL, delay);
    }

    /**
     * Указание завершить обработку задачи
     *
     * @return действие
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
