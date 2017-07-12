package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.impl.TransactionalEnqueuer;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;

/**
 * Постановщик задач в очередь, используемый в spring конфигурации.
 *
 * @param <T> тип данных задачи
 *
 * @author Oleg Kandaurov
 * @since 05.08.2017
 */
public class SpringTransactionalEnqueuer<T> extends SpringEnqueuer<T> {

    private TransactionalEnqueuer<T> enqueuer;

    /**
     * Конструктор постановщика задач
     *
     * @param queueLocation местоположение очереди
     * @param payloadClass  класс данных задачи
     */
    public SpringTransactionalEnqueuer(@Nonnull QueueLocation queueLocation, @Nonnull Class<T> payloadClass) {
        super(queueLocation, payloadClass);
    }

    @Override
    public Long enqueue(@Nonnull EnqueueParams<T> enqueueParams) {
        if (enqueuer == null) {
            enqueuer = new TransactionalEnqueuer<T>(getQueueConfig(), getPayloadTransformer(), getShards(),
                    getShardRouter());
        }
        return enqueuer.enqueue(enqueueParams);
    }
}
