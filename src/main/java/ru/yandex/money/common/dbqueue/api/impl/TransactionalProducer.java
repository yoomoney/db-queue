package ru.yandex.money.common.dbqueue.api.impl;

import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueProducer;
import ru.yandex.money.common.dbqueue.api.QueueShard;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Реализация постановщика задач в очередь.
 * <p>
 * Предоставляет логику роутинга и сохранение задачи в соответствующий шард.
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 05.08.2017
 */
public class TransactionalProducer<T> implements QueueProducer<T> {

    private final QueueConfig queueConfig;
    private final TaskPayloadTransformer<T> payloadTransformer;
    private final ProducerShardRouter<T> shardRouter;

    /**
     * Конструктор
     *
     * @param queueConfig        конфигурация очереди
     * @param payloadTransformer преобразователь данных задачи
     * @param shardRouter        правила роутинга задачи на шарды
     */
    public TransactionalProducer(@Nonnull QueueConfig queueConfig,
                                 @Nonnull TaskPayloadTransformer<T> payloadTransformer,
                                 @Nonnull ProducerShardRouter<T> shardRouter) {
        this.queueConfig = requireNonNull(queueConfig, "queueConfig");
        this.payloadTransformer = requireNonNull(payloadTransformer, "payloadTransformer");
        this.shardRouter = requireNonNull(shardRouter, "shardRouter");
    }

    @Override
    public long enqueue(@Nonnull EnqueueParams<T> enqueueParams) {
        requireNonNull(enqueueParams);
        QueueShard queueShard = shardRouter.resolveEnqueuingShard(enqueueParams);
        EnqueueParams<String> rawEnqueueParams = new EnqueueParams<String>()
                .withPayload(payloadTransformer.fromObject(enqueueParams.getPayload()))
                .withTraceInfo(enqueueParams.getTraceInfo())
                .withExecutionDelay(enqueueParams.getExecutionDelay())
                .withActor(enqueueParams.getActor());
        return queueShard.getTransactionTemplate().execute(status ->
                queueShard.getQueueDao().enqueue(queueConfig.getLocation(), rawEnqueueParams));
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<T> getPayloadTransformer() {
        return payloadTransformer;
    }

    @Nonnull
    @Override
    public ProducerShardRouter<T> getProducerShardRouter() {
        return shardRouter;
    }

    @Nonnull
    @Override
    public QueueConfig getQueueConfig() {
        return queueConfig;
    }

}
