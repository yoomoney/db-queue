package ru.yandex.money.common.dbqueue.config.impl;

import ru.yandex.money.common.dbqueue.config.QueueShardId;
import ru.yandex.money.common.dbqueue.config.ThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;

/**
 * Empty listener for task processing thread in the queue.
 *
 * @author Oleg Kandaurov
 * @since 02.10.2019
 */
public class NoopThreadLifecycleListener implements ThreadLifecycleListener {

    private static final NoopThreadLifecycleListener INSTANCE = new NoopThreadLifecycleListener();

    public static NoopThreadLifecycleListener getInstance() {
        return INSTANCE;
    }

    @Override
    public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {

    }

    @Override
    public void executed(QueueShardId shardId, QueueLocation location,
                         boolean taskProcessed, long threadBusyTime) {

    }

    @Override
    public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {

    }

    @Override
    public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                        @Nonnull Throwable exc) {

    }
}
