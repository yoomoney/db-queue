package ru.yandex.money.common.dbqueue.stub;

import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueProducer;
import ru.yandex.money.common.dbqueue.api.QueueShardRouter;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class FakeQueueProducer implements QueueProducer<String> {

    private final QueueConfig queueConfig;
    private final TaskPayloadTransformer<String> transformer;
    private final QueueShardRouter<String> shardRouter;
    private final Function<EnqueueParams<String>, Long> execFunc;

    public FakeQueueProducer(QueueConfig queueConfig, TaskPayloadTransformer<String> transformer,
                             QueueShardRouter<String> shardRouter, Function<EnqueueParams<String>, Long> execFunc) {
        this.queueConfig = queueConfig;
        this.transformer = transformer;
        this.shardRouter = shardRouter;
        this.execFunc = execFunc;
    }

    @Override
    public long enqueue(@Nonnull EnqueueParams<String> enqueueParams) {
        return execFunc.apply(enqueueParams);
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<String> getPayloadTransformer() {
        return transformer;
    }

    @Nonnull
    @Override
    public QueueConfig getQueueConfig() {
        return queueConfig;
    }

    @Nonnull
    @Override
    public QueueShardRouter<String> getProducerShardRouter() {
        return shardRouter;
    }
}
