package ru.yandex.money.common.dbqueue.api;

import ru.yandex.money.common.dbqueue.config.QueueTableSchema;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

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
    @Nonnull
    private Map<String, String> extData = new LinkedHashMap<>();

    /**
     * Создать параметры постановки с данными задачи.
     *
     * @param payload данные задачи
     * @param <R>     Тип данных задачи
     * @return объект с параметрами постановки задачи в очередь
     */
    public static <R> EnqueueParams<R> create(@Nonnull R payload) {
        requireNonNull(payload);
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
        this.executionDelay = requireNonNull(executionDelay);
        return this;
    }

    /**
     * Задать расширенный набор данных задачи
     *
     * @param columnName имя колонки
     * @param value      значение колонки
     * @return параметры постановки задачи в очередь
     */
    @Nonnull
    public EnqueueParams<T> withExtData(@Nonnull String columnName, @Nullable String value) {
        extData.put(requireNonNull(columnName), value);
        return this;
    }

    /**
     * Задать расширенный набор данных задачи.
     * Функционал включается через {@link QueueTableSchema#getExtFields()}
     *
     * @param extData набор расширенных данных, ключ - имя колонки в БД. Все элементы этой коллекции будут перемещены
     *                во внутреннюю коллекцию builder'а
     * @return параметры постановки задачи в очередь
     */
    @Nonnull
    public EnqueueParams<T> withExtData(@Nonnull Map<String, String> extData) {
        requireNonNull(extData);
        this.extData.putAll(extData);
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
     * Получить расширенный набор данных задачи
     *
     * @return дополнительные данные задачи, в ключе содержится имя колонки в БД
     */
    @Nonnull
    public Map<String, String> getExtData() {
        return Collections.unmodifiableMap(extData);
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
                Objects.equals(extData, that.extData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, executionDelay, extData);
    }

    @Override
    public String toString() {
        return '{' +
                "executionDelay=" + executionDelay +
                (payload != null ? ",payload=" + payload : "") +
                '}';
    }
}
