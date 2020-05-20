package ru.yandex.money.common.dbqueue.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Parameters with typed payload to enqueue the task
 *
 * @param <T> A type of the payload in the task
 * @author Oleg Kandaurov
 * @since 12.07.2017
 */
public final class EnqueueParams<T> {
    @Nullable
    private T payload;
    @Nonnull
    private Duration executionDelay = Duration.ZERO;
    @Nonnull
    private final Map<String, String> extData = new LinkedHashMap<>();

    /**
     * Create new task parameters with payload
     *
     * @param payload task payload
     * @param <R>     The type of the payload in the task
     * @return An object with task parameters and a payload
     */
    public static <R> EnqueueParams<R> create(@Nonnull R payload) {
        requireNonNull(payload);
        return new EnqueueParams<R>().withPayload(payload);
    }

    /**
     * Add a typed payload to the task parameters
     *
     * @param payload Task payload
     * @return A reference to the same object with added payload
     */
    @Nonnull
    public EnqueueParams<T> withPayload(@Nullable T payload) {
        this.payload = payload;
        return this;
    }

    /**
     * Add an execution delay for the task.
     * The given task will not be executed before current date and time plus the execution delay.
     *
     * @param executionDelay Execution delay, {@linkplain Duration#ZERO} if not set.
     * @return A reference to the same object with execution delay set.
     */
    @Nonnull
    public EnqueueParams<T> withExecutionDelay(@Nonnull Duration executionDelay) {
        this.executionDelay = requireNonNull(executionDelay);
        return this;
    }

    /**
     * Add the external user parameter for the task.
     * If the column name is already present in the external user parameters,
     * then the original value will be replaced by the new one.
     *
     * @param columnName The name of the user-defined column in tasks table.
     *                   The column <strong>must</strong> exist in the tasks table.
     * @param value      The value of the user-defined parameter
     * @return A reference to the same object of the task parameters with external user parameter.
     */
    @Nonnull
    public EnqueueParams<T> withExtData(@Nonnull String columnName, @Nullable String value) {
        extData.put(requireNonNull(columnName), value);
        return this;
    }

    /**
     * Update the task parameters with the map of external user-defined parameters,
     * a map where the key is the name of the user-defined column in tasks table,
     * and the value is the value of the user-defined parameter.
     *
     * @param extData Map of external user-defined parameters, key is the column name in the tasks table.
     *                All elements of that collection will be <strong>added</strong> to those
     *                already present in task parameters object,
     *                the value will replace the existing value on a duplicate key.
     * @return A reference to the same object of the task parameters with external user-defined parameters map.
     */
    @Nonnull
    public EnqueueParams<T> withExtData(@Nonnull Map<String, String> extData) {
        requireNonNull(extData);
        this.extData.putAll(extData);
        return this;
    }

    /**
     * Get task payload
     *
     * @return Typed task payload
     */
    @Nullable
    public T getPayload() {
        return payload;
    }

    /**
     * Get the task execution delay, a {@linkplain Duration#ZERO} is the default one if not set.
     *
     * @return Task execution delay.
     */
    @Nonnull
    public Duration getExecutionDelay() {
        return executionDelay;
    }

    /**
     * Get the <strong>unmodifiable</strong> map of extended user-defined parameters for the task:
     * a map where the key is the name of the user-defined column in tasks table,
     * and the value is the value of the user-defined parameter.
     *
     * @return Map of external user-defined parameters, where the key is the column name in the tasks table.
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
