package ru.yandex.money.common.dbqueue.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.internal.processing.LoopPolicy;
import ru.yandex.money.common.dbqueue.internal.processing.MillisTimeProvider;
import ru.yandex.money.common.dbqueue.internal.processing.QueueLoop;
import ru.yandex.money.common.dbqueue.internal.runner.QueueRunner;
import ru.yandex.money.common.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Пул управлящий запуском, приостановкой и завершением обработчиков очереди на заданном шарде
 *
 * @author Oleg Kandaurov
 * @since 14.07.2017
 */
class QueueExecutionPool {
    private static final Logger log = LoggerFactory.getLogger(QueueExecutionPool.class);

    @Nonnull
    private final QueueConsumer queueConsumer;
    @Nonnull
    private final QueueShard queueShard;
    @Nonnull
    private final QueueLoop queueLoop;
    @Nonnull
    private final ExecutorService executor;
    @Nonnull
    private final QueueRunner queueRunner;

    private boolean started = false;

    QueueExecutionPool(@Nonnull QueueConsumer queueConsumer,
                       @Nonnull QueueShard queueShard,
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

    QueueExecutionPool(@Nonnull QueueConsumer queueConsumer,
                       @Nonnull QueueShard queueShard,
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
     * Получить идентификатор шарда, который обрабатывает текущий пул.
     *
     * @return идентификатор шарда
     */
    QueueShardId getQueueShardId() {
        return queueShard.getShardId();
    }

    /**
     * Запустить обработку задач в очереди
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
     * Завершить обработку очереди, семантика аналогична {@link ExecutorService#shutdownNow()}
     */
    void shutdown() {
        log.info("shutting down queue: queueId={}, shardId={}", getQueueId(), queueShard.getShardId());
        executor.shutdownNow();
    }

    /**
     * Приостановить обработку очереди.
     * Запустить обработку вновь можно методом {@link QueueExecutionPool#start()}
     */
    void pause() {
        log.info("pausing queue: queueId={}, shardId={}", getQueueId(), queueShard.getShardId());
        queueLoop.pause();
    }

    /**
     * Получить признак, что очередь приостановлена методом {@link QueueExecutionPool#pause()}
     *
     * @return true если очередь приостановлена
     */
    boolean isPaused() {
        return queueLoop.isPaused();
    }

    /**
     * Получить признак, что обработка очереди завершена методом {@link QueueExecutionPool#shutdown()}.
     * Семантика аналогична {@link ExecutorService#isShutdown()}
     *
     * @return true если обработка очереди завершена
     */
    boolean isShutdown() {
        return executor.isShutdown();
    }

    /**
     * Получить признак, что все потоки обработки очереди завершились успехом.
     * Семантика аналогична {@link ExecutorService#isTerminated()}
     *
     * @return true если все потоки обработки очереди завершены
     */
    boolean isTerminated() {
        return executor.isTerminated();
    }

    /**
     * Дождаться завершения обработки задач в заданный таймаут.
     * Семантика аналогична {@link ExecutorService#awaitTermination(long, TimeUnit)}
     *
     * @param timeout таймаут ожидания
     * @return true если все задачи завершились в заданный таймаут
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
     * Принудительно продолжить обработку задач в очереди,
     * если обработка была приостановлена по событию
     * {@link ru.yandex.money.common.dbqueue.settings.QueueConfigsReader#SETTING_NO_TASK_TIMEOUT}
     */
    void wakeup() {
        queueLoop.wakeup();
    }

}
