package ru.yoomoney.tech.dbqueue.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.internal.processing.MillisTimeProvider;
import ru.yoomoney.tech.dbqueue.internal.processing.QueueLoop;
import ru.yoomoney.tech.dbqueue.internal.processing.QueueTaskPoller;
import ru.yoomoney.tech.dbqueue.internal.runner.QueueRunner;
import ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader;
import ru.yoomoney.tech.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Task execution pool: manages start, pause and shutdown of task executors on the assigned shard.
 *
 * @author Oleg Kandaurov
 * @since 14.07.2017
 */
class QueueExecutionPool {
    private static final Logger log = LoggerFactory.getLogger(QueueExecutionPool.class);

    @Nonnull
    private final QueueConsumer<?> queueConsumer;
    @Nonnull
    private final QueueShard<?> queueShard;
    @Nonnull
    private final QueueTaskPoller queueTaskPoller;
    @Nonnull
    private final ExecutorService executor;
    @Nonnull
    private final QueueRunner queueRunner;
    @Nonnull
    private final Supplier<QueueLoop> queueLoopFactory;

    private final List<QueueWorker> queueWorkers = new ArrayList<>();

    private boolean started = false;

    QueueExecutionPool(@Nonnull QueueConsumer<?> queueConsumer,
                       @Nonnull QueueShard<?> queueShard,
                       @Nonnull TaskLifecycleListener taskLifecycleListener,
                       @Nonnull ThreadLifecycleListener threadLifecycleListener) {
        this(queueConsumer, queueShard,
                new QueueTaskPoller(threadLifecycleListener,
                        new MillisTimeProvider.SystemMillisTimeProvider()),
                new ThreadPoolExecutor(
                        queueConsumer.getQueueConfig().getSettings().getThreadCount(),
                        Integer.MAX_VALUE,
                        1L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        new QueueThreadFactory(
                                queueConsumer.getQueueConfig().getLocation(), queueShard.getShardId())),
                QueueRunner.Factory.create(queueConsumer, queueShard, taskLifecycleListener),
                QueueLoop.WakeupQueueLoop::new);
    }

    QueueExecutionPool(@Nonnull QueueConsumer<?> queueConsumer,
                       @Nonnull QueueShard<?> queueShard,
                       @Nonnull QueueTaskPoller queueTaskPoller,
                       @Nonnull ExecutorService executor,
                       @Nonnull QueueRunner queueRunner,
                       @Nonnull Supplier<QueueLoop> queueLoopFactory) {
        this.queueConsumer = requireNonNull(queueConsumer);
        this.queueShard = requireNonNull(queueShard);
        this.queueTaskPoller = requireNonNull(queueTaskPoller);
        this.executor = requireNonNull(executor);
        this.queueRunner = requireNonNull(queueRunner);
        this.queueLoopFactory = requireNonNull(queueLoopFactory);
    }

    private QueueId getQueueId() {
        return queueConsumer.getQueueConfig().getLocation().getQueueId();
    }

    /**
     * Get identifier of the shard, which will be managed with the execution pool.
     *
     * @return Shard identifier.
     */
    QueueShardId getQueueShardId() {
        return queueShard.getShardId();
    }

    /**
     * Start task processing in the queue
     */
    void start() {
        if (!started) {
            started = true;
            log.info("starting queue: queueId={}, shardId={}", getQueueId(), queueShard.getShardId());
            resizePool(queueConsumer.getQueueConfig().getSettings().getThreadCount());
        }
    }

    /**
     * Resize queue execution pool in runtime
     *
     * @param newThreadCount thread count for execution pool.
     */
    void resizePool(int newThreadCount) {
        int oldThreadCount = queueWorkers.size();
        log.info("resizing queue execution pool: queueId={}, shardId={}, oldThreadCount={}, " +
                        "newThreadCount={}",
                queueConsumer.getQueueConfig().getLocation().getQueueId(),
                queueShard.getShardId(), oldThreadCount, newThreadCount);
        if (newThreadCount > oldThreadCount) {
            for (int i = oldThreadCount; i < newThreadCount; i++) {
                startThread();
            }
        } else if (newThreadCount < oldThreadCount) {
            for (int i = oldThreadCount; i > newThreadCount; i--) {
                disposeThread();
            }
        }
        if (executor instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
            threadPoolExecutor.setCorePoolSize(newThreadCount);
            threadPoolExecutor.allowCoreThreadTimeOut(true);
            threadPoolExecutor.purge();
        }

    }

    private void startThread() {
        QueueLoop queueLoop = queueLoopFactory.get();
        Future<?> future = executor.submit(() -> queueTaskPoller.start(queueLoop,
                queueShard.getShardId(), queueConsumer, queueRunner));
        queueWorkers.add(new QueueWorker(future, queueLoop));
        if (started) {
            queueLoop.unpause();
        }
    }

    private void disposeThread() {
        QueueWorker queueWorker = queueWorkers.get(queueWorkers.size() - 1);
        queueWorker.getFuture().cancel(true);
        queueWorkers.remove(queueWorkers.size() - 1);
    }

    /**
     * Stop tasks processing, semantic is the same as for {@link ExecutorService#shutdownNow()}
     */
    void shutdown() {
        log.info("shutting down queue: queueId={}, shardId={}", getQueueId(), queueShard.getShardId());
        executor.shutdownNow();
    }

    /**
     * Pause task processing.
     * To start the processing again, use {@link QueueExecutionPool#start()} method
     */
    void pause() {
        log.info("pausing queue: queueId={}, shardId={}", getQueueId(), queueShard.getShardId());
        queueWorkers.forEach(queueWorker -> queueWorker.getLoop().pause());
    }

    /**
     * Get attribute that the tasks processing was paused with {@link QueueExecutionPool#pause()} method.
     *
     * @return true if the tasks processing was paused.
     */
    boolean isPaused() {
        return queueWorkers.stream().anyMatch(queueWorker -> queueWorker.getLoop().isPaused());
    }

    /**
     * Get attribute that the tasks processing was stopped with {@link QueueExecutionPool#shutdown()} method.
     * Semantic is the same as for {@link ExecutorService#isShutdown()}.
     *
     * @return true if the tasks processing was stopped.
     */
    boolean isShutdown() {
        return executor.isShutdown();
    }

    /**
     * Get attribute that all the processing threads were successfully terminated.
     * Semantic is the same as for {@link ExecutorService#isTerminated()}.
     *
     * @return true if all the threads were successfully terminated.
     */
    boolean isTerminated() {
        return executor.isTerminated();
    }

    /**
     * Wait for tasks (and threads) termination within given timeout.
     * Semantic is the same as for {@link ExecutorService#awaitTermination(long, TimeUnit)}.
     *
     * @param timeout waiting timeout
     * @return true if all the threads were successfully terminated within given timeout.
     */
    boolean awaitTermination(@Nonnull Duration timeout) {
        requireNonNull(timeout, "timeout");
        log.info("awaiting queue termination: queueId={}, shardId={}, timeout={}",
                getQueueId(), queueShard.getShardId(), timeout);
        try {
            return executor.awaitTermination(timeout.getSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Force continue task processing if processing was paused
     * with {@link QueueConfigsReader#SETTING_NO_TASK_TIMEOUT} event.
     */
    void wakeup() {
        queueWorkers.forEach(queueWorker -> queueWorker.getLoop().doContinue());
    }

    private static class QueueWorker {
        @Nonnull
        private final Future<?> future;
        @Nonnull
        private final QueueLoop loop;

        private QueueWorker(@Nonnull Future<?> future, @Nonnull QueueLoop loop) {
            this.future = Objects.requireNonNull(future);
            this.loop = Objects.requireNonNull(loop);
        }

        @Nonnull
        public Future<?> getFuture() {
            return future;
        }

        @Nonnull
        public QueueLoop getLoop() {
            return loop;
        }
    }

}
