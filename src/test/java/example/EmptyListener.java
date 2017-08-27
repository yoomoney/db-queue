package example;

import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.ThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;

/**
 * @author Oleg Kandaurov
 * @since 14.08.2017
 */
class EmptyListener implements ThreadLifecycleListener {

    @Override
    public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {
    }

    @Override
    public void executed(QueueShardId shardId, QueueLocation location, boolean taskProcessed, long threadBusyTime) {
    }

    @Override
    public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {
    }

    @Override
    public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull Throwable exc) {
    }
}
