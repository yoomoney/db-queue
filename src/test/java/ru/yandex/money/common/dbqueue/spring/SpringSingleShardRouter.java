package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueShard;
import ru.yandex.money.common.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;

/**
 * @author Oleg Kandaurov
 * @since 13.08.2018
 */
public class SpringSingleShardRouter<T> extends SpringQueueShardRouter<T> {

    private final QueueShard queueShard;

    public SpringSingleShardRouter(QueueId queueId, Class<T> payloadClass, QueueShard queueShard) {
        super(queueId, payloadClass);
        this.queueShard = queueShard;
    }

    @Nonnull
    @Override
    public Collection<QueueShard> getProcessingShards() {
        return Collections.singletonList(queueShard);
    }

    @Nonnull
    @Override
    public QueueShard resolveEnqueuingShard(@Nonnull EnqueueParams<T> enqueueParams) {
        return queueShard;
    }
}
