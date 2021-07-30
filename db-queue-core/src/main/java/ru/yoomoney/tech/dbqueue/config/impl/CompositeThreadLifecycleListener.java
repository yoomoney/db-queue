package ru.yoomoney.tech.dbqueue.config.impl;

import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.ThreadLifecycleListener;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Composite listener. It allows combining several listeners into one.
 *
 *  Listeners for started events is executed in straight order.
 *  Listeners for executed, finished and crashed events are executed in reverse order.
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class CompositeThreadLifecycleListener implements ThreadLifecycleListener {

    @Nonnull
    private final List<ThreadLifecycleListener> listeners;
    @Nonnull
    private final List<ThreadLifecycleListener> reverseListeners;

    /**
     * Constructor
     *
     * @param listeners thread listeners
     */
    public CompositeThreadLifecycleListener(@Nonnull List<ThreadLifecycleListener> listeners) {
        this.listeners = Objects.requireNonNull(listeners, "listeners must not be null");
        this.reverseListeners = new ArrayList<>(listeners);
        Collections.reverse(reverseListeners);
    }

    @Override
    public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {
        listeners.forEach(l -> l.started(shardId, location));
    }

    @Override
    public void executed(QueueShardId shardId, QueueLocation location, boolean taskProcessed, long threadBusyTime) {
        reverseListeners.forEach(l -> l.executed(shardId, location, taskProcessed, threadBusyTime));
    }

    @Override
    public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {
        reverseListeners.forEach(l -> l.finished(shardId, location));
    }

    @Override
    public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nullable Throwable exc) {
        reverseListeners.forEach(l -> l.crashed(shardId, location, exc));
    }
}
