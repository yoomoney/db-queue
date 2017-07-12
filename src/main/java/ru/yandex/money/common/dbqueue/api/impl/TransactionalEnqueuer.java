package ru.yandex.money.common.dbqueue.api.impl;

import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.ShardRouter;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.api.Enqueuer;
import ru.yandex.money.common.dbqueue.api.PayloadTransformer;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Реализация постановщика задач в очередь.
 *
 * Предоставляет логику роутинга и сохранение задачи в соответствующий шард.
 *
 * @param <T> тип данных задачи
 *
 * @author Oleg Kandaurov
 * @since 05.08.2017
 */
public class TransactionalEnqueuer<T> implements Enqueuer<T> {

    private final QueueConfig queueConfig;
    private final PayloadTransformer<T> payloadTransformer;
    private final Map<QueueShardId, QueueDao> shards;
    private final ShardRouter<T> shardRouter;

    /**
     * Конструктор
     *
     * @param queueConfig конфигурация очереди
     * @param payloadTransformer преобразователь данных задачи
     * @param shards шарды, доступные для роутинга
     * @param shardRouter правила роутинга задачи на шарды
     */
    public TransactionalEnqueuer(QueueConfig queueConfig, PayloadTransformer<T> payloadTransformer,
                                 Collection<QueueDao> shards, ShardRouter<T> shardRouter) {
        this.queueConfig = queueConfig;
        this.payloadTransformer = payloadTransformer;
        this.shards = shards.stream().collect(Collectors.toMap(QueueDao::getShardId, Function.identity()));
        this.shardRouter = shardRouter;
    }

    @Override
    public Long enqueue(@Nonnull EnqueueParams<T> enqueueParams) {
        Objects.requireNonNull(enqueueParams);
        QueueDao queueDao = shards.get(shardRouter.resolveShardId(enqueueParams));
        EnqueueParams<String> rawEnqueueParams = new EnqueueParams<String>()
                .withPayload(payloadTransformer.fromObject(enqueueParams.getPayload()))
                .withCorrelationId(enqueueParams.getCorrelationId())
                .withExecutionDelay(enqueueParams.getExecutionDelay())
                .withActor(enqueueParams.getActor());
        return queueDao.getTransactionTemplate().execute(status ->
                queueDao.enqueue(queueConfig.getLocation(), rawEnqueueParams));
    }

    @Nonnull
    @Override
    public PayloadTransformer<T> getPayloadTransformer() {
        return payloadTransformer;
    }

    @Nonnull
    @Override
    public ShardRouter<T> getShardRouter() {
        return shardRouter;
    }

    @Nonnull
    @Override
    public QueueConfig getQueueConfig() {
        return queueConfig;
    }

}
