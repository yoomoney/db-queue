package ru.yoomoney.tech.dbqueue.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.internal.processing.MillisTimeProvider;
import ru.yoomoney.tech.dbqueue.internal.processing.TimeLimiter;
import ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader;
import ru.yoomoney.tech.dbqueue.settings.QueueId;

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
 * A service for managing start, pause and shutdown of task processors.
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
    private final BiFunction<QueueShard, QueueConsumer<?>, QueueExecutionPool> queueExecutionPoolFactory;

    public QueueService(@Nonnull List<QueueShard> queueShards,
                        @Nonnull ThreadLifecycleListener threadLifecycleListener,
                        @Nonnull TaskLifecycleListener taskLifecycleListener) {
        this(queueShards,
                (shard, consumer) -> new QueueExecutionPool(consumer, shard,
                        taskLifecycleListener, threadLifecycleListener));
    }

    QueueService(@Nonnull List<QueueShard> queueShards,
                 @Nonnull BiFunction<QueueShard, QueueConsumer<?>, QueueExecutionPool> queueExecutionPoolFactory) {
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
     * Register new task processor of given payload type.
     *
     * @param consumer Task processor.
     * @param <T>      Type of the processor (type of the payload in the task).
     * @return Attribute of successful task processor registration.
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
     * Start tasks processing in all queues registered in the service.
     */
    public void start() {
        log.info("starting all queues");
        registeredQueues.keySet().forEach(this::start);
    }

    /**
     * Start tasks processing in one given queue.
     *
     * @param queueId Queue identifier.
     */
    public void start(@Nonnull QueueId queueId) {
        requireNonNull(queueId, "queueId");
        log.info("starting queue: queueId={}", queueId);
        getQueuePools(queueId, "start").values().forEach(QueueExecutionPool::start);
    }

    /**
     * Stop tasks processing in all queues registered in the service,
     * semantic is the same as for {@link ExecutorService#shutdownNow()}.
     */
    public void shutdown() {
        log.info("shutting down all queues");
        registeredQueues.keySet().forEach(this::shutdown);
    }

    /**
     * Stop tasks processing in one given queue,
     * semantic is the same as for {@link ExecutorService#shutdownNow()}.
     *
     * @param queueId Queue identifier.
     */
    public void shutdown(@Nonnull QueueId queueId) {
        requireNonNull(queueId, "queueId");
        log.info("shutting down queue: queueId={}", queueId);
        getQueuePools(queueId, "shutdown").values().forEach(QueueExecutionPool::shutdown);
    }

    /**
     * Get attribute that the tasks processing was stopped in one specific queue
     * with {@link QueueService#shutdown()} method.
     * Semantic is the same as for {@link ExecutorService#isShutdown()}.
     *
     * @param queueId Queue identifier.
     * @return true if the tasks processing was stopped.
     */
    public boolean isShutdown(@Nonnull QueueId queueId) {
        requireNonNull(queueId, "queueId");
        return getQueuePools(queueId, "isShutdown").values().stream()
                .allMatch(QueueExecutionPool::isShutdown);
    }

    /**
     * Get attribute that the tasks processing was stopped in all registered queues
     * with {@link QueueService#shutdown()}.
     * Semantic is the same as for {@link ExecutorService#isShutdown()}.
     *
     * @return true if the tasks processing was stopped.
     */
    public boolean isShutdown() {
        return registeredQueues.keySet().stream().allMatch(this::isShutdown);
    }

    /**
     * Get attribute that all the processing task threads were successfully terminated in the specified queue.
     * Semantic is the same as for {@link ExecutorService#isTerminated()}.
     *
     * @param queueId Queue identifier.
     * @return true if all the task threads were terminated in specified queue.
     */
    public boolean isTerminated(@Nonnull QueueId queueId) {
        requireNonNull(queueId, "queueId");
        return getQueuePools(queueId, "isTerminated").values().stream().allMatch(QueueExecutionPool::isTerminated);
    }

    /**
     * Get attribute that all queues finished their execution and all task threads were terminated.
     * Semantic is the same as for {@link ExecutorService#isTerminated()}.
     *
     * @return true if all task threads in all queues were terminated.
     */
    public boolean isTerminated() {
        return registeredQueues.keySet().stream().allMatch(this::isTerminated);
    }

    /**
     * Pause task processing in specified queue.
     * To start the processing again, use {{@link QueueService#start(QueueId)} method.
     *
     * @param queueId Queue identifier.
     */
    public void pause(@Nonnull QueueId queueId) {
        requireNonNull(queueId, "queueId");
        log.info("pausing queue: queueId={}", queueId);
        getQueuePools(queueId, "pause").values().forEach(QueueExecutionPool::pause);
    }

    /**
     * Pause task processing in all queues.
     * To start the processing again, use {@link QueueService#start()} method.
     */
    public void pause() {
        log.info("pausing all queues");
        registeredQueues.keySet().forEach(this::pause);
    }

    /**
     * Get attribute that all queues were paused with {@link QueueService#pause()} method.
     *
     * @return true if queues were paused.
     */
    public boolean isPaused() {
        return registeredQueues.keySet().stream().allMatch(this::isPaused);
    }

    /**
     * Get attribute that the specified queue were paused with {@link QueueService#pause(QueueId)} method.
     *
     * @param queueId Queue identifier.
     * @return true if specified queue were paused.
     */
    public boolean isPaused(@Nonnull QueueId queueId) {
        requireNonNull(queueId, "queueId");
        return getQueuePools(queueId, "isPaused").values().stream()
                .allMatch(QueueExecutionPool::isPaused);
    }

    /**
     * Wait for tasks (and threads) termination in all queues within given timeout.
     * Semantic is the same as for {@link ExecutorService#awaitTermination(long, TimeUnit)}.
     *
     * @param timeout Wait timeout.
     * @return List of queues, which didn't stop their work (didn't terminate).
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
     * Wait for tasks (and threads) termination in specified queue within given timeout.
     * Semantic is the same as for {@link ExecutorService#awaitTermination(long, TimeUnit)}.
     *
     * @param queueId Queue identifier.
     * @param timeout Wait timeout.
     * @return List of shards, where the work didn't stop (working threads on which were not terminated).
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
     * Force continue task processing in specified queue by given shard identifier.
     * <p>
     * Processing continues only if the queue were paused with
     * {@link QueueConfigsReader#SETTING_NO_TASK_TIMEOUT} event.
     * <p>
     * It might be useful for queues which interact with the end user,
     * whereas the end users might often expect possibly the quickest response on their actions.
     * Applies right after a task enqueue,
     * therefore should be called only after successful task insertion transaction.
     * Applies also to tests to improve the speed of test execution.
     *
     * @param queueId      Queue identifier.
     * @param queueShardId Shard identifier.
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
