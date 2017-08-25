package ru.yandex.money.common.dbqueue.api;

import ru.yandex.money.common.dbqueue.init.QueueRegistry;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;

/**
 * Постановщик задач в очередь.
 * <p>
 * При регистрации посредством {@link QueueRegistry}:
 * <p>
 * 1) Позволяет убедится, что есть очередь, обработающая поставленную задачу
 * 2) В случае типизированных данных задачи гарантирует, что они корректно десериализуются очередью
 * 3) Гарантирует, что правила шардирования совпадают с соответствующими правилами очереди
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

    /**
     * Получить конфигурацию очереди, обработающую поставленную задачу.
     *
     * @return конфигурация очереди
     */
    @Nonnull
    QueueConfig getQueueConfig();

    /**
     * Получить правила шардирования задач, обрабатываемых очередью
     *
     * @return правила шардирования
     */
    @Nonnull
    QueueShardRouter<T> getShardRouter();

}
