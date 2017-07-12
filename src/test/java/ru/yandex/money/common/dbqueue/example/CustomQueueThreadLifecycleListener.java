package ru.yandex.money.common.dbqueue.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.QueueThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;

/**
 * @author Oleg Kandaurov
 * @since 17.07.2017
 */
public class CustomQueueThreadLifecycleListener implements QueueThreadLifecycleListener {

    private static final Logger log = LoggerFactory.getLogger(CustomQueueThreadLifecycleListener.class);

    @Override
    public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {
    }

    @Override
    public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {
    }

    @Override
    public void crashedPickTask(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull Throwable exc) {
        log.error("fatal error while processing: location={}", location);
    }
}
