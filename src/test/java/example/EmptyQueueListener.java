package example;

import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.QueueThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;

/**
 * @author Oleg Kandaurov
 * @since 14.08.2017
 */
class EmptyQueueListener implements QueueThreadLifecycleListener {

    @Override
    public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {
    }

    @Override
    public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {
    }

    @Override
    public void crashedPickTask(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull Throwable exc) {
    }
}
