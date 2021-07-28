package ru.yoomoney.tech.dbqueue.api.impl;

import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.EnqueueResult;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.api.QueueShardRouter;
import ru.yoomoney.tech.dbqueue.api.TaskPayloadTransformer;
import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Wrapper for queue producer wrapper with sharding support.
 *
 * @param <PayloadTaskT>         The type of the payload in the task
 * @param <DatabaseAccessLayerT> The type of the database access layer
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class ShardingQueueProducer<PayloadTaskT, DatabaseAccessLayerT extends DatabaseAccessLayer>
        implements QueueProducer<PayloadTaskT> {

    @Nonnull
    private final QueueShardRouter<PayloadTaskT, DatabaseAccessLayerT> queueShardRouter;
    @Nonnull
    private final TaskPayloadTransformer<PayloadTaskT> payloadTransformer;
    @Nonnull
    private final QueueConfig queueConfig;

    /**
     * Constructor
     *
     * @param queueConfig        Configuration of the queue
     * @param payloadTransformer Transformer of a payload data
     * @param queueShardRouter   Dispatcher for sharding support
     */
    public ShardingQueueProducer(@Nonnull QueueConfig queueConfig,
                                 @Nonnull TaskPayloadTransformer<PayloadTaskT> payloadTransformer,
                                 @Nonnull QueueShardRouter<PayloadTaskT, DatabaseAccessLayerT> queueShardRouter) {
        this.queueShardRouter = Objects.requireNonNull(queueShardRouter);
        this.payloadTransformer = Objects.requireNonNull(payloadTransformer);
        this.queueConfig = Objects.requireNonNull(queueConfig);
    }

    @Override
    public EnqueueResult enqueue(@Nonnull EnqueueParams<PayloadTaskT> enqueueParams) {
        QueueShard<DatabaseAccessLayerT> queueShard = queueShardRouter.resolveShard(enqueueParams);
        EnqueueParams<String> rawEnqueueParams = new EnqueueParams<String>()
                .withPayload(payloadTransformer.fromObject(enqueueParams.getPayload()))
                .withExecutionDelay(enqueueParams.getExecutionDelay())
                .withExtData(enqueueParams.getExtData());
        Long enqueueId = queueShard.getDatabaseAccessLayer().transact(() ->
                queueShard.getDatabaseAccessLayer().getQueueDao().enqueue(queueConfig.getLocation(), rawEnqueueParams));
        return EnqueueResult.builder()
                .withShardId(queueShard.getShardId())
                .withEnqueueId(enqueueId)
                .build();
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<PayloadTaskT> getPayloadTransformer() {
        return payloadTransformer;
    }
}
