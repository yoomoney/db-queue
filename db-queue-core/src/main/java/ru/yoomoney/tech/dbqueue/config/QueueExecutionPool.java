package ru.yoomoney.tech.dbqueue.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.internal.processing.LoopPolicy;
import ru.yoomoney.tech.dbqueue.internal.processing.MillisTimeProvider;
import ru.yoomoney.tech.dbqueue.internal.processing.QueueLoop;
import ru.yoomoney.tech.dbqueue.internal.runner.QueueRunner;
import ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader;
import ru.yoomoney.tech.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    private final QueueLoop queueLoop;
    @Nonnull
    private final ExecutorService executor;
    @Nonnull
    private final QueueRunner queueRunner;

    private boolean started = false;

    QueueExecutionPool(@Nonnull QueueConsumer<?> queueConsumer,
                       @Nonnull QueueShard<?> queueShard,
                       @Nonnull TaskLifecycleListener taskLifecycleListener,
                       @Nonnull ThreadLifecycleListener threadLifecycleListener) {
        this(queueConsumer, queueShard,
                new QueueLoop(new LoopPolicy.WakeupLoopPolicy(), threadLifecycleListener,
                        new MillisTimeProvider.SystemMillisTimeProvider()),
                new ThreadPoolExecutor(
                        queueConsumer.getQueueConfig().getSettings().getThreadCount(),
                        queueConsumer.getQueueConfig().getSettings().getThreadCount(),
                        0L, TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(
                                queueConsumer.getQueueConfig().getSettings().getThreadCount()),
                        new QueueThreadFactory(
                                queueConsumer.getQueueConfig().getLocation(), queueShard.getShardId())),
                QueueRunner.Factory.create(queueConsumer, queueShard, taskLifecycleListener));
    }

    QueueExecutionPool(@Nonnull QueueConsumer<?> queueConsumer,
                       @Nonnull QueueShard<?> queueShard,
                       @Nonnull QueueLoop queueLoop,
                       @Nonnull ExecutorService executor,
                       @Nonnull QueueRunner queueRunner) {
        this.queueConsumer = requireNonNull(queueConsumer);
        this.queueShard = requireNonNull(queueShard);
        this.queueLoop = requireNonNull(queueLoop);
        this.executor = requireNonNull(executor);
        this.queueRunner = requireNonNull(queueRunner);
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
            log.info("starting queue loop: queueId={}, shardId={}", getQueueId(), queueShard.getShardId());
            for (int i = 0; i < queueConsumer.getQueueConfig().getSettings().getThreadCount(); i++) {
                executor.execute(() -> queueLoop.start(queueShard.getShardId(), queueConsumer, queueRunner));
            }
            started = true;
        }
        log.info("starting queue: queueId={}, shardId={}", getQueueId(), queueShard.getShardId());
        queueLoop.unpause();
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
        queueLoop.pause();
    }

    /**
     * Get attribute that the tasks processing was paused with {@link QueueExecutionPool#pause()} method.
     *
     * @return true if the tasks processing was paused.
     */
    boolean isPaused() {
        return queueLoop.isPaused();
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
        queueLoop.wakeup();
    }

}
