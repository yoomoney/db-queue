package ru.yandex.money.common.dbqueue.api;

import javax.annotation.Nonnull;

/**
 * Постановщик задач в очередь.
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public interface QueueProducer<T> {

    /**
     * Поместить задачу в очередь.
     *
     * @param enqueueParams параметры постановки задачи в очередь
     * @return идентификатор (sequence id) вставленной задачи
     */
    long enqueue(@Nonnull EnqueueParams<T> enqueueParams);

    /**
     * Предоставить преобразователь данных задачи.
     *
     * @return преобразователь данных
     */
    @Nonnull
    TaskPayloadTransformer<T> getPayloadTransformer();

}
