package ru.yandex.money.common.dbqueue.api;

import ru.yandex.money.common.dbqueue.dao.QueueActorDao;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Objects;

/**
 * Данные для постановки задачи в очередь.
 *
 * @param <T> тип данных в задаче
 * @author Oleg Kandaurov
 * @since 12.07.2017
 */
public final class EnqueueParams<T> {
    @Nullable
    private T payload;
    @Nonnull
    private Duration executionDelay = Duration.ZERO;
    @Nullable
    private String traceInfo;
    @Nullable
    private String actor;

    /**
     * Создать параметры постановки с данными задачи.
     *
     * @param payload данные задачи
     * @param <R>     Тип данных задачи
     * @return объект с параметрами постановки задачи в очередь
     */
    public static <R> EnqueueParams<R> create(@Nonnull R payload) {
        Objects.requireNonNull(payload);
        return new EnqueueParams<R>().withPayload(payload);
    }

    /**
     * Задать данные задачи.
     *
     * @param payload данные задачи
     * @return параметры постановки задачи в очередь
     */
    @Nonnull
    public EnqueueParams<T> withPayload(@Nullable T payload) {
        this.payload = payload;
        return this;
    }

    /**
     * Задать задержку выполнения очереди.
     *
     * @param executionDelay задержка выполнения
     * @return параметры постановки задачи в очередь
     */
    @Nonnull
    public EnqueueParams<T> withExecutionDelay(@Nonnull Duration executionDelay) {
        this.executionDelay = Objects.requireNonNull(executionDelay);
        return this;
    }

    /**
     * Нетипизированные данные трассировки. К примеру информация в формате open tracing.
     *
     * @param traceInfo данные трассировки
     * @return параметры постановки задачи в очередь
     */
    @Nonnull
    public EnqueueParams<T> withTraceInfo(@Nullable String traceInfo) {
        this.traceInfo = traceInfo;
        return this;
    }

    /**
     * Бизнесовый идентификатор очереди.
     * В качестве подобного идентификатор можно выбрать ключ сущности, подлежащей обработки
     * <p>
     * Может быть также использован для получения данных и внешнего управления очередями
     * посредством {@link QueueActorDao}
     *
     * @param actor идентификатор
     * @return параметры постановки задачи в очередь
     */
    @Nonnull
    public EnqueueParams<T> withActor(@Nullable String actor) {
        this.actor = actor;
        return this;
    }

    /**
     * Получить данные задачи
     *
     * @return данные задачи
     */
    @Nullable
    public T getPayload() {
        return payload;
    }

    /**
     * Получить задержку выполнения задачи
     *
     * @return задержка выполнения задачи
     */
    @Nonnull
    public Duration getExecutionDelay() {
        return executionDelay;
    }

    /**
     * Получить данные трассировки
     *
     * @return данные трассировки
     */
    @Nullable
    public String getTraceInfo() {
        return traceInfo;
    }

    /**
     * Получить бизнесовый идентификатор
     *
     * @return бизнесовый идентификатор
     */
    @Nullable
    public String getActor() {
        return actor;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        EnqueueParams<?> that = (EnqueueParams<?>) obj;
        return Objects.equals(payload, that.payload) &&
                Objects.equals(executionDelay, that.executionDelay) &&
                Objects.equals(traceInfo, that.traceInfo) &&
                Objects.equals(actor, that.actor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, executionDelay, traceInfo, actor);
    }

    @Override
    public String toString() {
        return '{' +
                "executionDelay=" + executionDelay +
                (payload != null ? ",payload=" + payload : "") +
                (actor != null ? ", actor=" + actor : "") +
                '}';
    }
}
