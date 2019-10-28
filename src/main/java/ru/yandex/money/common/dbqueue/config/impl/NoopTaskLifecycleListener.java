package ru.yandex.money.common.dbqueue.config.impl;

import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.config.QueueShardId;
import ru.yandex.money.common.dbqueue.config.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;

/**
 * Пустой слушатель событий задачи
 *
 * @author Oleg Kandaurov
 * @since 02.10.2019
 */
public final class NoopTaskLifecycleListener implements TaskLifecycleListener {

    private static final NoopTaskLifecycleListener INSTANCE = new NoopTaskLifecycleListener();

    public static NoopTaskLifecycleListener getInstance() {
        return INSTANCE;
    }

    @Override
    public void picked(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                       @Nonnull TaskRecord taskRecord, long pickTaskTime) {

    }

    @Override
    public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                        @Nonnull TaskRecord taskRecord) {

    }

    @Override
    public void executed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                         @Nonnull TaskRecord taskRecord, @Nonnull TaskExecutionResult executionResult, long processTaskTime) {

    }

    @Override
    public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                         @Nonnull TaskRecord taskRecord) {

    }

    @Override
    public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location,
                        @Nonnull TaskRecord taskRecord, @Nonnull Exception exc) {

    }
}
