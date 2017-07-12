package ru.yandex.money.common.dbqueue.init;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.api.Queue;
import ru.yandex.money.common.dbqueue.api.QueueThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.internal.LoopPolicy;
import ru.yandex.money.common.dbqueue.internal.QueueLoop;
import ru.yandex.money.common.dbqueue.internal.runner.QueueRunner;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Класс обеспечивающий запуск и остановку очередей, полученных из хранилища {@link QueueRegistry}
 *
 * @author Oleg Kandaurov
 * @since 14.07.2017
 */
@SuppressWarnings({"rawtypes", "unchecked", "AccessToStaticFieldLockedOnInstance", "SynchronizedMethod"})
public class QueueExecutionPool {
    private static final Logger log = LoggerFactory.getLogger(QueueExecutionPool.class);

    private final Map<QueueLocation, Map<QueueShardId, ExecutorService>> queueThreadPools = new HashMap<>();
    @Nonnull
    private final QueueRegistry queueRegistry;
    @Nonnull
    private final TaskLifecycleListener defaultTaskLifecycleListener;
    @Nonnull
    private final QueueThreadLifecycleListener queueThreadLifecycleListener;

    private volatile boolean started;
    private static final Duration TERMINATION_TIMEOUT = Duration.ofSeconds(30L);

    /**
     * Конструктор
     *
     * @param queueRegistry хранилище очередей
     * @param defaultTaskLifecycleListener слушатель жизненного цикла задачи
     * @param queueThreadLifecycleListener слушатель жизненного цикла потока очереди
     */
    public QueueExecutionPool(@Nonnull QueueRegistry queueRegistry,
                              @Nonnull TaskLifecycleListener defaultTaskLifecycleListener,
                              @Nonnull QueueThreadLifecycleListener queueThreadLifecycleListener) {
        this.queueRegistry = queueRegistry;
        this.defaultTaskLifecycleListener = defaultTaskLifecycleListener;
        this.queueThreadLifecycleListener = queueThreadLifecycleListener;
    }

    /**
     * Запустить обработку очередей
     */
    public synchronized void start() {
        if (started) {
            throw new IllegalStateException("queues already started");
        }
        started = true;
        log.info("starting queues");
        queueRegistry.getQueues().forEach(this::createAndStartThreads);
    }

    /**
     * Остановить обработку очередей
     */
    public synchronized void shutdown() {
        if (!started) {
            throw new IllegalStateException("queues already stopped");
        }
        started = false;

        log.info("shutting down queues");
        // Необходимо сначала остановить очереди выборки задач
        // В противном случае будут поступать задачи в executor,
        // который не сможет их принять.
        queueThreadPools.entrySet().parallelStream().forEach(queuePools -> {
            log.info("shutting down queue: location={}", queuePools.getKey());
            Map<QueueShardId, ExecutorService> shardThreadPools = queuePools.getValue();
            shardThreadPools.entrySet().parallelStream().forEach(shardPools ->
                    shutdownThreadPool(shardPools.getKey(), shardPools.getValue())
            );
        });
        queueRegistry.getExternalExecutors().entrySet().parallelStream().forEach(entry -> {
            log.info("shutting down external executor: location={}", entry.getKey());
            entry.getValue().shutdownQueueExecutor();
        });
        log.info("all queue threads stopped");
    }

    private synchronized void createAndStartThreads(@Nonnull Queue queue) {
        Objects.requireNonNull(queue);
        QueueConfig config = queue.getQueueConfig();
        if (config.getSettings().getThreadCount() <= 0) {
            log.info("queue is turned off: location={}", config.getLocation());
            return;
        }

        TaskLifecycleListener taskListener = queueRegistry.getTaskListeners()
                .getOrDefault(queue.getQueueConfig().getLocation(), defaultTaskLifecycleListener);
        Executor externalExecutor = queueRegistry.getExternalExecutors().get(queue.getQueueConfig().getLocation());

        Map<QueueShardId, ExecutorService> shardThreadPools = new HashMap<>();
        queueThreadPools.put(config.getLocation(), shardThreadPools);
        Collection<QueueShardId> shards = queue.getShardRouter().getShardsId();
        for (QueueShardId shardId : shards) {

            ExecutorService queueThreadPool = Executors.newFixedThreadPool(
                    config.getSettings().getThreadCount(),
                    new QueueThreadFactory(config.getLocation(), shardId));

            QueueLoop queueLoop = new QueueLoop(new LoopPolicy.ThreadLoopPolicy(), queueThreadLifecycleListener);

            QueueDao queueDao = queueRegistry.getShards().get(shardId);

            QueueRunner queueRunner = QueueRunner.Factory.createQueueRunner(queue,
                    queueDao, taskListener, externalExecutor);

            shardThreadPools.put(shardId, queueThreadPool);
            for (int i = 0; i < config.getSettings().getThreadCount(); i++) {
                queueThreadPool.execute(() -> queueLoop.start(shardId, queue, queueRunner));
            }
        }

    }


    private static void shutdownThreadPool(@Nonnull QueueShardId shardId, @Nonnull ExecutorService executorService) {
        Objects.requireNonNull(shardId);
        Objects.requireNonNull(executorService);
        log.info("waiting queue threads to respond to interrupt request: shardId={}, timeout={}",
                shardId, TERMINATION_TIMEOUT);
        try {
            executorService.shutdownNow();
            if (!executorService.awaitTermination(TERMINATION_TIMEOUT.getSeconds(), TimeUnit.SECONDS)) {
                log.error("several queue threads are still not terminated: shardId={}", shardId);
            }
        } catch (InterruptedException ignored) {
            log.error("queue shutdown is unexpectedly terminated");
            Thread.currentThread().interrupt();
        }
    }

    private static class QueueThreadFactory implements ThreadFactory {

        private static final AtomicInteger POOL_NUMBER = new AtomicInteger(0);
        private static final String THREAD_FACTORY_NAME = "queue";
        private final ThreadFactory backingThreadFactory = Executors.defaultThreadFactory();
        private final String namePrefix;
        private final AtomicInteger threadNumber = new AtomicInteger(0);
        private final Thread.UncaughtExceptionHandler exceptionHandler =
                new QueueUncaughtExceptionHandler();
        private final QueueLocation location;
        private final QueueShardId shardId;

        @SuppressFBWarnings("STT_TOSTRING_STORED_IN_FIELD")
        QueueThreadFactory(@Nonnull QueueLocation location, @Nonnull QueueShardId shardId) {
            Objects.requireNonNull(location);
            Objects.requireNonNull(shardId);
            this.location = location;
            this.shardId = shardId;
            this.namePrefix = THREAD_FACTORY_NAME + '-' + Integer.toString(POOL_NUMBER.getAndIncrement()) + '-';
        }


        @Override
        public Thread newThread(@Nonnull Runnable runnable) {
            Thread thread = backingThreadFactory.newThread(runnable);
            String threadName = namePrefix + threadNumber.getAndIncrement();
            thread.setName(threadName);
            thread.setUncaughtExceptionHandler(exceptionHandler);
            log.info("created queue thread: location={}, shardId={}, threadName={}", location, shardId, threadName);
            return thread;
        }
    }

    private static class QueueUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread thread, Throwable throwable) {
            log.error("detected uncaught exception", throwable);
        }
    }

}
