package ru.yandex.money.common.dbqueue.spring.impl;

import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.impl.TransactionalProducer;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.spring.SpringQueueProducer;

import javax.annotation.Nonnull;

/**
 * Постановщик задач в очередь, используемый в spring конфигурации.
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 05.08.2017
 */
public class SpringTransactionalProducer<T> extends SpringQueueProducer<T> {

    private TransactionalProducer<T> producer;

    /**
     * Конструктор постановщика задач
     *
     * @param queueId идентификатор очереди
     * @param payloadClass  класс данных задачи
     */
    public SpringTransactionalProducer(@Nonnull QueueId queueId, @Nonnull Class<T> payloadClass) {
        super(queueId, payloadClass);
    }

    @Override
    public long enqueue(@Nonnull EnqueueParams<T> enqueueParams) {
        if (producer == null) {
            producer = new TransactionalProducer<>(getQueueConfig(), getPayloadTransformer(), getShards(),
                    getShardRouter());
        }
        return producer.enqueue(enqueueParams);
    }
}
