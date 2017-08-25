package ru.yandex.money.common.dbqueue.api;

import javax.annotation.Nullable;

/**
 * Сериализатор/десериализатор данных в очереди.
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public interface TaskPayloadTransformer<T> {

    /**
     * Преобразовать строковые данные задачи в объект.
     *
     * @param payload данные задачи
     * @return объект с данными задачи
     */
    @Nullable
    T toObject(@Nullable String payload);

    /**
     * Преобразовать объект с данными задачи в строку.
     *
     * @param payload данные задачи
     * @return строка с данными задачи
     */
    @Nullable
    String fromObject(@Nullable T payload);

}
