package ru.yandex.money.common.dbqueue.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.money.common.dbqueue.api.QueueAction;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;

/**
 * @author Oleg Kandaurov
 * @since 12.07.2017
 */
public class CustomTaskLifecycleListener implements TaskLifecycleListener {

    private static final Logger log = LoggerFactory.getLogger(CustomTaskLifecycleListener.class);

    @Override
    public void picked(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord, long pickTaskTime) {
        log.info("task selection time: {}", pickTaskTime);
    }

    @Override
    public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord) {

    }

    @Override
    public void executed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord,
                         @Nonnull QueueAction executionResult, long processTaskTime) {
        log.info("execution time: {}", processTaskTime);
    }

    @Override
    public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord) {

    }

    @Override
    public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord, @Nonnull Exception exc) {
        log.error("error while processing task: payload={}", taskRecord.getPayload(), exc);
    }
}
