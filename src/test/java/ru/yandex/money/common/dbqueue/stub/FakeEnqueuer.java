package ru.yandex.money.common.dbqueue.stub;

import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.PayloadTransformer;
import ru.yandex.money.common.dbqueue.api.ShardRouter;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.api.Enqueuer;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class FakeEnqueuer implements Enqueuer<String> {

    private final QueueConfig queueConfig;
    private final PayloadTransformer<String> transformer;
    private final ShardRouter<String> shardRouter;
    private final Function<EnqueueParams<String>, Long> execFunc;

    public FakeEnqueuer(QueueConfig queueConfig, PayloadTransformer<String> transformer,
                     ShardRouter<String> shardRouter, Function<EnqueueParams<String>, Long> execFunc) {
        this.queueConfig = queueConfig;
        this.transformer = transformer;
        this.shardRouter = shardRouter;
        this.execFunc = execFunc;
    }

    @Override
    public Long enqueue(@Nonnull EnqueueParams<String> enqueueParams) {
        return execFunc.apply(enqueueParams);
    }

    @Nonnull
    @Override
    public PayloadTransformer<String> getPayloadTransformer() {
        return transformer;
    }

    @Nonnull
    @Override
    public QueueConfig getQueueConfig() {
        return queueConfig;
    }

    @Nonnull
    @Override
    public ShardRouter<String> getShardRouter() {
        return shardRouter;
    }
}
