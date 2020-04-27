package ru.yandex.money.common.dbqueue.api;

import javax.annotation.Nullable;

/**
 * Marshaller and unmarshaller for the payload in the task
 *
 * @param <T> The type of the payload in the task
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public interface TaskPayloadTransformer<T> {

    /**
     * Unmarshall the string payload from the task into the object with task data
     *
     * @param payload task payload
     * @return Object with task data
     */
    @Nullable
    T toObject(@Nullable String payload);

    /**
     * Marshall the typed object with task parameters into string payload.
     *
     * @param payload task payload
     * @return string with the task payload.
     */
    @Nullable
    String fromObject(@Nullable T payload);

}
