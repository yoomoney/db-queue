package ru.yoomoney.tech.dbqueue.example;

import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.api.TaskPayloadTransformer;
import ru.yoomoney.tech.dbqueue.api.impl.NoopPayloadTransformer;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.spring.dao.SpringDatabaseAccessLayer;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Queue producer without sharding and payload transformation
 *
 * @author Oleg Kandaurov
 * @since 02.10.2019
 */
public class StringQueueProducer implements QueueProducer<String> {

    @Nonnull
    private final QueueConfig queueConfig;
    @Nonnull
    private final QueueShard<SpringDatabaseAccessLayer> queueShard;

    public StringQueueProducer(@Nonnull QueueConfig queueConfig,
                               @Nonnull QueueShard<SpringDatabaseAccessLayer> queueShard) {
        this.queueConfig = requireNonNull(queueConfig, "queueConfig");
        this.queueShard = requireNonNull(queueShard, "queueShard");
    }

    @Override
    public long enqueue(@Nonnull EnqueueParams<String> enqueueParams) {
        requireNonNull(enqueueParams);
        EnqueueParams<String> rawEnqueueParams = new EnqueueParams<String>()
                .withPayload(getPayloadTransformer().fromObject(enqueueParams.getPayload()))
                .withExecutionDelay(enqueueParams.getExecutionDelay())
                .withExtData(enqueueParams.getExtData());
        return requireNonNull(queueShard.getDatabaseAccessLayer().transact(() ->
                queueShard.getDatabaseAccessLayer().getQueueDao().enqueue(queueConfig.getLocation(), rawEnqueueParams)));
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<String> getPayloadTransformer() {
        return NoopPayloadTransformer.getInstance();
    }

}
