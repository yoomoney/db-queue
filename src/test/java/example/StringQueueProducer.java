package example;

import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueProducer;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.api.impl.NoopPayloadTransformer;
import ru.yandex.money.common.dbqueue.config.QueueShard;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;

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
    private final QueueShard queueShard;

    public StringQueueProducer(@Nonnull QueueConfig queueConfig,
                               @Nonnull QueueShard queueShard) {
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
        return requireNonNull(queueShard.getTransactionTemplate().execute(status ->
                queueShard.getQueueDao().enqueue(queueConfig.getLocation(), rawEnqueueParams)));
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<String> getPayloadTransformer() {
        return NoopPayloadTransformer.getInstance();
    }

}
