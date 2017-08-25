package ru.yandex.money.common.dbqueue.spring.impl;

import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.impl.TransactionalProducer;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
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
     * @param queueLocation местоположение очереди
     * @param payloadClass  класс данных задачи
     */
    public SpringTransactionalProducer(@Nonnull QueueLocation queueLocation, @Nonnull Class<T> payloadClass) {
        super(queueLocation, payloadClass);
    }

    @Override
    public Long enqueue(@Nonnull EnqueueParams<T> enqueueParams) {
        if (producer == null) {
            producer = new TransactionalProducer<T>(getQueueConfig(), getPayloadTransformer(), getShards(),
                    getShardRouter());
        }
        return producer.enqueue(enqueueParams);
    }
}
