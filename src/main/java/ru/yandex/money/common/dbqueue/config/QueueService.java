package ru.yandex.money.common.dbqueue.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.internal.processing.MillisTimeProvider;
import ru.yandex.money.common.dbqueue.internal.processing.TimeLimiter;
import ru.yandex.money.common.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Сервис управлящий регистрацией, запуском, приостановкой и завершением обработчиков очередей
 *
 * @author Oleg Kandaurov
 * @since 14.07.2017
 */
public class QueueService {
    private static final Logger log = LoggerFactory.getLogger(QueueService.class);

    @Nonnull
    private final Map<QueueId, Map<QueueShardId, QueueExecutionPool>> registeredQueues = new LinkedHashMap<>();
    @Nonnull
    private final List<QueueShard> queueShards;
    @Nonnull
    private final BiFunction<QueueShard, QueueConsumer, QueueExecutionPool> queueExecutionPoolFactory;

    public QueueService(@Nonnull List<QueueShard> queueShards,
                        @Nonnull ThreadLifecycleListener threadLifecycleListener,
                        @Nonnull TaskLifecycleListener taskLifecycleListener) {
        this(queueShards,
                (shard, consumer) -> new QueueExecutionPool(consumer, shard,
                        taskLifecycleListener, threadLifecycleListener));
    }

    QueueService(@Nonnull List<QueueShard> queueShards,
                 @Nonnull BiFunction<QueueShard, QueueConsumer, QueueExecutionPool> queueExecutionPoolFactory) {
        this.queueShards = requireNonNull(queueShards, "queueShards");
        this.queueExecutionPoolFactory = requireNonNull(queueExecutionPoolFactory, "queueExecutionPoolFactory");
    }

    private Map<QueueShardId, QueueExecutionPool> getQueuePools(@Nonnull QueueId queueId,
                                                                @Nonnull String method) {
        requireNonNull(queueId, "queueId");
        requireNonNull(method, "method");
        if (!registeredQueues.containsKey(queueId)) {
            throw new IllegalArgumentException("cannot invoke " + method +
                    ", queue is not registered: queueId=" + queueId);
        }
        return registeredQueues.get(queueId);
    }

    /**
     * Зарегистрировать обработчик очереди
     *
     * @param consumer обработчик очереди
     * @param <T>      тип обработчика
     * @return признак успеха регистрации
     */
    public <T> boolean registerQueue(@Nonnull QueueConsumer<T> consumer) {
        requireNonNull(consumer);
        QueueId queueId = consumer.getQueueConfig().getLocation().getQueueId();
        if (registeredQueues.containsKey(queueId)) {
            log.info("queue is already registered: queueId={}", queueId);
            return false;
        }

        int threadCount = consumer.getQueueConfig().getSettings().getThreadCount();
        if (threadCount <= 0) {
            log.info("queue is turned off, skipping registration: queueId={}", queueId);
            return false;
        }

        Map<QueueShardId, QueueExecutionPool> queueShardPools = new LinkedHashMap<>();
        queueShards.forEach(shard -> queueShardPools.put(shard.getShardId(),
                queueExecutionPoolFactory.apply(shard, consumer)));
        registeredQueues.put(queueId, queueShardPools);
        return true;
    }

    /**
     * Запустить обработку всех очередей
     */
    public void start() {
        log.info("starting all queues");
        registeredQueues.keySet().forEach(this::start);
    }

    /**
     * Запустить обработку одной очереди
     *
     * @param queueId идентификатор очереди
     */
    public void start(@Nonnull QueueId queueId) {
        requireNonNull(queueId, "queueId");
        log.info("starting queue: queueId={}", queueId);
        getQueuePools(queueId, "start").values().forEach(QueueExecutionPool::start);
    }

    /**
     * Завершить обработку всех очередей, семантика аналогична {@link ExecutorService#shutdownNow()}
     */
    public void shutdown() {
        log.info("shutting down all queues");
        registeredQueues.keySet().forEach(this::shutdown);
    }

    /**
     * Завершить обработку очереди, семантика аналогична {@link ExecutorService#shutdownNow()}
     *
     * @param queueId идентификатор очереди
     */
    public void shutdown(@Nonnull QueueId queueId) {
        requireNonNull(queueId, "queueId");
        log.info("shutting down queue: queueId={}", queueId);
        getQueuePools(queueId, "shutdown").values().forEach(QueueExecutionPool::shutdown);
    }

    /**
     * Получить признак, что обработка очереди завершена методом {@link QueueService#shutdown()}.
     * Семантика аналогична {@link ExecutorService#isShutdown()}
     *
     * @param queueId идентификатор очереди
     * @return true если обработка очереди завершена
     */
    public boolean isShutdown(@Nonnull QueueId queueId) {
        requireNonNull(queueId, "queueId");
        return getQueuePools(queueId, "isShutdown").values().stream()
                .allMatch(QueueExecutionPool::isShutdown);
    }

    /**
     * Получить признак, что обработка всех очередей завершена методом {@link QueueService#shutdown()}.
     * Семантика аналогична {@link ExecutorService#isShutdown()}
     *
     * @return true если обработка очередей завершена
     */
    public boolean isShutdown() {
        return registeredQueues.keySet().stream().allMatch(this::isShutdown);
    }

    /**
     * Получить признак, что очередь завершила обработку задач.
     * Семантика аналогична {@link ExecutorService#isTerminated()}
     *
     * @param queueId идентификатор очереди
     * @return true если все в задачи в очереди завершены
     */
    public boolean isTerminated(@Nonnull QueueId queueId) {
        requireNonNull(queueId, "queueId");
        return getQueuePools(queueId, "isTerminated").values().stream().allMatch(QueueExecutionPool::isTerminated);
    }

    /**
     * Получить признак, что все очереди завершили обработку задач.
     * Семантика аналогична {@link ExecutorService#isTerminated()}
     *
     * @return true если все в задачи во всех очередях завершены
     */
    public boolean isTerminated() {
        return registeredQueues.keySet().stream().allMatch(this::isTerminated);
    }

    /**
     * Приостановить обработку очереди.
     * Запустить обработку вновь можно методом {@link QueueService#start(QueueId)}
     *
     * @param queueId идентификатор очереди
     */
    public void pause(@Nonnull QueueId queueId) {
        requireNonNull(queueId, "queueId");
        log.info("pausing queue: queueId={}", queueId);
        getQueuePools(queueId, "pause").values().forEach(QueueExecutionPool::pause);
    }

    /**
     * Приостановить обработку всех очереди.
     * Запустить обработку вновь можно методом {@link QueueService#start()}
     */
    public void pause() {
        log.info("pausing all queues");
        registeredQueues.keySet().forEach(this::pause);
    }

    /**
     * Получить признак, что все очереди приостановлены методом {@link QueueService#pause()}
     *
     * @return true если очереди приостановлена
     */
    public boolean isPaused() {
        return registeredQueues.keySet().stream().allMatch(this::isPaused);
    }

    /**
     * Получить признак, что очередь приостановлена методом {@link QueueService#pause(QueueId)}
     *
     * @param queueId идентификатор очереди
     * @return true если очередь приостановлена
     */
    public boolean isPaused(@Nonnull QueueId queueId) {
        requireNonNull(queueId, "queueId");
        return getQueuePools(queueId, "isPaused").values().stream()
                .allMatch(QueueExecutionPool::isPaused);
    }

    /**
     * Дождаться завершения обработки всех задач всех очередей в заданный таймаут.
     * Семантика аналогична {@link ExecutorService#awaitTermination(long, TimeUnit)}
     *
     * @param timeout таймаут ожидания
     * @return список очередей, которые не завершили работу
     */
    public List<QueueId> awaitTermination(@Nonnull Duration timeout) {
        requireNonNull(timeout, "timeout");
        log.info("awaiting all queues termination: timeout={}", timeout);
        TimeLimiter timeLimiter = new TimeLimiter(new MillisTimeProvider.SystemMillisTimeProvider(), timeout);
        registeredQueues.keySet().forEach(queueId ->
                timeLimiter.execute(remainingTimeout -> awaitTermination(queueId, remainingTimeout)));
        return registeredQueues.keySet().stream().filter(queueId -> !isTerminated(queueId)).collect(Collectors.toList());
    }

    /**
     * Дождаться завершения обработки задач очереди в заданный таймаут.
     * Семантика аналогична {@link ExecutorService#awaitTermination(long, TimeUnit)}
     *
     * @param queueId идентификатор очереди
     * @param timeout таймаут ожидания
     * @return список шардов на которых не была завершена работа
     */
    public List<QueueShardId> awaitTermination(@Nonnull QueueId queueId, @Nonnull Duration timeout) {
        requireNonNull(queueId, "queueId");
        requireNonNull(timeout, "timeout");
        log.info("awaiting queue termination: queueId={}, timeout={}", queueId, timeout);
        TimeLimiter timeLimiter = new TimeLimiter(new MillisTimeProvider.SystemMillisTimeProvider(), timeout);
        getQueuePools(queueId, "awaitTermination").values()
                .forEach(queueExecutionPool -> timeLimiter.execute(queueExecutionPool::awaitTermination));
        return getQueuePools(queueId, "awaitTermination").values().stream()
                .filter(queueExecutionPool -> !queueExecutionPool.isTerminated())
                .map(QueueExecutionPool::getQueueShardId)
                .collect(Collectors.toList());
    }

    /**
     * Принудительно продолжить обработку задач в очереди.
     * <p>
     * Продолжение обработки происходит только если очередь была приостановлена по событию
     * {@link ru.yandex.money.common.dbqueue.settings.QueueConfigsReader#SETTING_NO_TASK_TIMEOUT}
     * <p>
     * Может быть полезно для очередей, которые относятся к взаимодействию с пользователем,
     * поскольку пользователю зачастую требуется быстрый отклик на свои действия.
     * Используется после постановки задачи на выполнение, поэтому следует делать только после завершения транзакции
     * по вставке задачи. Также возможно применение в тестах для их ускорения.
     *
     * @param queueId      идентификатор очереди
     * @param queueShardId идентфикатор шарда
     */
    public void wakeup(@Nonnull QueueId queueId, @Nonnull QueueShardId queueShardId) {
        requireNonNull(queueId, "queueId");
        requireNonNull(queueShardId, "queueShardId");
        Map<QueueShardId, QueueExecutionPool> queuePools = getQueuePools(queueId, "wakeup");
        QueueExecutionPool queueExecutionPool = queuePools.get(queueShardId);
        if (queueExecutionPool == null) {
            throw new IllegalArgumentException("cannot wakeup, unknown shard: " +
                    "queueId=" + queueId + ", shardId=" + queueShardId);
        }
        queueExecutionPool.wakeup();
    }

}
