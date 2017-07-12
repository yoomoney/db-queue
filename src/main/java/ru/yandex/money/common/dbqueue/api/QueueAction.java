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
public class QueueAction {

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

    private static final QueueAction FINISH_ACTION = new QueueAction(Type.FINISH);
    private static final QueueAction FAIL_ACTION = new QueueAction(Type.FAIL);

    @Nonnull
    private final Type actionType;
    @Nullable
    private final Duration executionDelay;

    private QueueAction(@Nonnull Type actionType, @Nullable Duration executionDelay) {
        this.actionType = Objects.requireNonNull(actionType);
        this.executionDelay = executionDelay;
    }

    private QueueAction(@Nonnull Type actionType) {
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
    public static QueueAction reenqueue(@Nonnull Duration delay) {
        Objects.requireNonNull(delay);
        return new QueueAction(Type.REENQUEUE, delay);
    }

    /**
     * Указание повторить задачу позже
     * @return действие
     */
    @Nonnull
    public static QueueAction fail() {
        return FAIL_ACTION;
    }

    /**
     * Указание повторить задачу позже через фиксированное время
     * @param delay значение задержки
     * @return действие
     */
    @Nonnull
    public static QueueAction fail(@Nonnull Duration delay) {
        Objects.requireNonNull(delay);
        return new QueueAction(Type.FAIL, delay);
    }

    /**
     * Указание завершить обработку задачи
     * @return действие
     */
    @Nonnull
    public static QueueAction finish() {
        return FINISH_ACTION;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        QueueAction that = (QueueAction) obj;
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
