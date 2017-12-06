package ru.yandex.money.common.dbqueue.init;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.api.ThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.internal.LoopPolicy;
import ru.yandex.money.common.dbqueue.internal.MillisTimeProvider;
import ru.yandex.money.common.dbqueue.internal.QueueLoop;
import ru.yandex.money.common.dbqueue.internal.runner.QueueRunner;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Класс обеспечивающий запуск и остановку очередей, полученных из хранилища {@link QueueRegistry}
 *
 * @author Oleg Kandaurov
 * @since 14.07.2017
 */
@SuppressWarnings({"rawtypes", "unchecked", "AccessToStaticFieldLockedOnInstance", "SynchronizedMethod"})
public class QueueExecutionPool {
    private static final Logger log = LoggerFactory.getLogger(QueueExecutionPool.class);

    @Nonnull
    private final Collection<ShardPoolInstance> poolInstances = new ArrayList<>();
    @Nonnull
    private final QueueRegistry queueRegistry;
    @Nonnull
    private final TaskLifecycleListener defaultTaskLifecycleListener;
    @Nonnull
    private final ThreadLifecycleListener defaultThreadLifecycleListener;
    @Nonnull
    private final BiFunction<QueueLocation, QueueShardId, ThreadFactory> threadFactoryProvider;
    @Nonnull
    private final BiFunction<Integer, ThreadFactory, ExecutorService> queueThreadPoolFactory;
    @Nonnull
    private final Function<ThreadLifecycleListener, QueueLoop> queueLoopFactory;
    @Nonnull
    private final Function<ShardPoolInstance, QueueRunner> queueRunnerFactory;

    private volatile boolean started;
    private volatile boolean initialized;
    private static final Duration TERMINATION_TIMEOUT = Duration.ofSeconds(30L);

    /**
     * Конструктор
     *
     * @param queueRegistry                  хранилище очередей
     * @param defaultTaskLifecycleListener   слушатель жизненного цикла задачи
     * @param defaultThreadLifecycleListener слушатель жизненного цикла потока очереди
     */
    public QueueExecutionPool(@Nonnull QueueRegistry queueRegistry,
                              @Nonnull TaskLifecycleListener defaultTaskLifecycleListener,
                              @Nonnull ThreadLifecycleListener defaultThreadLifecycleListener) {
        this(queueRegistry, defaultTaskLifecycleListener, defaultThreadLifecycleListener,
                QueueThreadFactory::new,
                (threadCount, factory) -> new ThreadPoolExecutor(threadCount, threadCount,
                        0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(threadCount), factory),
                listener -> new QueueLoop(new LoopPolicy.ThreadLoopPolicy(), listener,
                        new MillisTimeProvider.SystemMillisTimeProvider()),
                poolInstance -> QueueRunner.Factory.createQueueRunner(poolInstance.queueConsumer,
                        poolInstance.queueDao, poolInstance.taskListener, poolInstance.externalExecutor));
    }

    /**
     * Конструктор для тестирования
     *
     * @param queueRegistry                  хранилище очередей
     * @param defaultTaskLifecycleListener   слушатель жизненного цикла задачи
     * @param defaultThreadLifecycleListener слушатель жизненного цикла потока очереди
     * @param threadFactoryProvider          фабрика фабрик создания потоков
     * @param queueThreadPoolFactory         фабрика для создания пула обработки очередей
     * @param queueLoopFactory               фабрика для создания {@link QueueLoop}
     * @param queueRunnerFactory             фабрика для создания {@link QueueRunner}
     */
    QueueExecutionPool(@Nonnull QueueRegistry queueRegistry,
                       @Nonnull TaskLifecycleListener defaultTaskLifecycleListener,
                       @Nonnull ThreadLifecycleListener defaultThreadLifecycleListener,
                       @Nonnull BiFunction<QueueLocation, QueueShardId, ThreadFactory> threadFactoryProvider,
                       @Nonnull BiFunction<Integer, ThreadFactory, ExecutorService> queueThreadPoolFactory,
                       @Nonnull Function<ThreadLifecycleListener, QueueLoop> queueLoopFactory,
                       @Nonnull Function<ShardPoolInstance, QueueRunner> queueRunnerFactory) {
        this.queueRegistry = Objects.requireNonNull(queueRegistry);
        this.defaultTaskLifecycleListener = Objects.requireNonNull(defaultTaskLifecycleListener);
        this.defaultThreadLifecycleListener = Objects.requireNonNull(defaultThreadLifecycleListener);
        this.queueThreadPoolFactory = Objects.requireNonNull(queueThreadPoolFactory);
        this.threadFactoryProvider = Objects.requireNonNull(threadFactoryProvider);
        this.queueLoopFactory = Objects.requireNonNull(queueLoopFactory);
        this.queueRunnerFactory = Objects.requireNonNull(queueRunnerFactory);
    }


    /**
     * Инициализировать пул очередей
     *
     * @return инстанс текущего объекта
     */
    public synchronized QueueExecutionPool init() {
        if (initialized) {
            throw new IllegalStateException("pool already initialized");
        }
        queueRegistry.getConsumers().forEach(this::initQueue);
        initialized = true;
        return this;
    }

    private synchronized void initQueue(@Nonnull QueueConsumer queueConsumer) {
        Objects.requireNonNull(queueConsumer);
        QueueId queueId = queueConsumer.getQueueConfig().getLocation().getQueueId();
        TaskLifecycleListener taskListener = queueRegistry.getTaskListeners()
                .getOrDefault(queueId, defaultTaskLifecycleListener);
        ThreadLifecycleListener threadListener = queueRegistry.getThreadListeners()
                .getOrDefault(queueId, defaultThreadLifecycleListener);
        Executor externalExecutor = queueRegistry.getExternalExecutors().get(queueId);
        Collection<QueueShardId> shards = queueConsumer.getShardRouter().getShardsId();
        for (QueueShardId shardId : shards) {
            QueueDao queueDao = queueRegistry.getShards().get(shardId);
            poolInstances.add(new ShardPoolInstance(queueConsumer, queueDao, taskListener, threadListener,
                    externalExecutor));
        }
    }

    /**
     * Запустить обработку очередей
     */
    public synchronized void start() {
        if (!initialized) {
            throw new IllegalStateException("pool is not initialized");
        }
        if (started) {
            throw new IllegalStateException("queues already started");
        }
        started = true;
        log.info("starting queues");
        poolInstances.forEach(poolInstance -> {
            QueueConfig queueConfig = poolInstance.queueConsumer.getQueueConfig();

            ThreadFactory threadFactory = threadFactoryProvider.apply(queueConfig.getLocation(),
                    poolInstance.queueDao.getShardId());
            int threadCount = queueConfig.getSettings().getThreadCount();
            if (threadCount <= 0) {
                log.info("queue is turned off: queueId={}",
                        queueConfig.getLocation().getQueueId());
            } else {
                ExecutorService shardThreadPool = queueThreadPoolFactory.apply(threadCount, threadFactory);
                poolInstance.queueShardThreadPool = shardThreadPool;
                QueueLoop queueLoop = queueLoopFactory.apply(poolInstance.threadListener);
                QueueRunner queueRunner = queueRunnerFactory.apply(poolInstance);

                for (int i = 0; i < threadCount; i++) {
                    shardThreadPool.execute(() -> queueLoop.start(poolInstance.queueDao.getShardId(),
                            poolInstance.queueConsumer, queueRunner));
                }
            }
        });
    }

    /**
     * Остановить обработку очередей
     */
    public synchronized void shutdown() {
        if (!initialized) {
            throw new IllegalStateException("pool is not initialized");
        }
        if (!started) {
            throw new IllegalStateException("queues already stopped");
        }
        started = false;

        log.info("shutting down queues");
        // Необходимо сначала остановить очереди выборки задач
        // В противном случае будут поступать задачи в executor,
        // который не сможет их принять.
        poolInstances.parallelStream().forEach(poolInstance -> {
            if (poolInstance.queueShardThreadPool != null) {
                log.info("shutting down queue: queueId={}, shardId={}",
                        poolInstance.queueConsumer.getQueueConfig().getLocation(),
                        poolInstance.queueDao.getShardId());
                shutdownThreadPool(poolInstance);
            }
        });
        queueRegistry.getExternalExecutors().entrySet().parallelStream().forEach(entry -> {
            log.info("shutting down external executor: queueId={}", entry.getKey().asString());
            entry.getValue().shutdownQueueExecutor();
        });
        log.info("all queue threads stopped");
    }

    private static void shutdownThreadPool(@Nonnull ShardPoolInstance poolInstance) {
        Objects.requireNonNull(poolInstance);
        Objects.requireNonNull(poolInstance.queueShardThreadPool);
        log.info("waiting queue threads to respond to interrupt request: queueId={}, shardId={}, timeout={}",
                poolInstance.queueConsumer.getQueueConfig().getLocation().getQueueId(),
                poolInstance.queueDao.getShardId(), TERMINATION_TIMEOUT);
        try {
            poolInstance.queueShardThreadPool.shutdownNow();
            if (!poolInstance.queueShardThreadPool.awaitTermination(
                    TERMINATION_TIMEOUT.getSeconds(), TimeUnit.SECONDS)) {
                log.error("several queue threads are still not terminated: queueId={}, shardId={}",
                        poolInstance.queueConsumer.getQueueConfig().getLocation().getQueueId(),
                        poolInstance.queueDao.getShardId());
            }
        } catch (InterruptedException ignored) {
            log.error("queue shutdown is unexpectedly terminated");
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Данные запускаемого обработчика очереди
     */
    static class ShardPoolInstance {
        /**
         * Очередь
         */
        final QueueConsumer queueConsumer;
        /**
         * Шард
         */
        final QueueDao queueDao;
        /**
         * Слушатель задач
         */
        final TaskLifecycleListener taskListener;
        /**
         * Слушатель потоков
         */
        final ThreadLifecycleListener threadListener;
        /**
         * Внешний исполнитель
         */
        final Executor externalExecutor;
        @Nullable
        private ExecutorService queueShardThreadPool;

        private ShardPoolInstance(QueueConsumer queueConsumer, QueueDao queueDao, TaskLifecycleListener taskListener,
                                  ThreadLifecycleListener threadListener, Executor externalExecutor) {
            this.queueConsumer = queueConsumer;
            this.queueDao = queueDao;
            this.taskListener = taskListener;
            this.threadListener = threadListener;
            this.externalExecutor = externalExecutor;
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
            log.info("created queue thread: threadName={}, location={}, shardId={}", threadName, location, shardId);
            return thread;
        }
    }

    private static class QueueUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

        @Override
        public void uncaughtException(Thread thread, Throwable throwable) {
            log.error("detected uncaught exception", throwable);
        }
    }

    /**
     * Получить реестр очередей
     *
     * @return реестр очередей
     */
    @Nonnull
    public QueueRegistry getQueueRegistry() {
        return queueRegistry;
    }
}
