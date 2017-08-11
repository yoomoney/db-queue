package ru.yandex.money.common.dbqueue.stub;

import ru.yandex.money.common.dbqueue.api.PayloadTransformer;
import ru.yandex.money.common.dbqueue.api.Queue;
import ru.yandex.money.common.dbqueue.api.QueueAction;
import ru.yandex.money.common.dbqueue.api.ShardRouter;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class FakeQueue implements Queue<String> {

    private final QueueConfig queueConfig;
    private final PayloadTransformer<String> transformer;
    private final ShardRouter<String> shardRouter;
    private final Function<Task<String>, QueueAction> execFunc;

    public FakeQueue(QueueConfig queueConfig, PayloadTransformer<String> transformer,
                     ShardRouter<String> shardRouter, Function<Task<String>, QueueAction> execFunc) {
        this.queueConfig = queueConfig;
        this.transformer = transformer;
        this.shardRouter = shardRouter;
        this.execFunc = execFunc;
    }

    @Nonnull
    @Override
    public QueueAction execute(@Nonnull Task<String> task) {
        return execFunc.apply(task);
    }

    @Nonnull
    @Override
    public QueueConfig getQueueConfig() {
        return queueConfig;
    }

    @Nonnull
    @Override
    public PayloadTransformer<String> getPayloadTransformer() {
        return transformer;
    }

    @Nonnull
    @Override
    public ShardRouter<String> getShardRouter() {
        return shardRouter;
    }
}
