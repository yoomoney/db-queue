package ru.yandex.money.common.dbqueue.init;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.money.common.dbqueue.api.Enqueuer;
import ru.yandex.money.common.dbqueue.api.Queue;
import ru.yandex.money.common.dbqueue.api.QueueExternalExecutor;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.ShardRouter;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Хранилище очередей.
 * <p>
 * Хранит не только очереди, а также объекты, относящиеся к данной очереди.
 * Данный класс предоставляет единое место контроля за корректностью конфигурации очередей.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class QueueRegistry {

    private static final Logger log = LoggerFactory.getLogger(QueueRegistry.class);

    private final Map<QueueLocation, Queue> queues = new LinkedHashMap<>();
    private final Map<QueueLocation, TaskLifecycleListener> taskListeners = new LinkedHashMap<>();
    private final Map<QueueLocation, QueueExternalExecutor> externalExecutors = new LinkedHashMap<>();
    private final Map<QueueShardId, QueueDao> shards = new LinkedHashMap<>();
    private final Collection<String> errorMessages = new ArrayList<>();

    private volatile boolean isRegistrationFinished;

    /**
     * Зарегистрировать очередь.
     *
     * @param <T>      тип данных задачи
     * @param queue    обработчик очереди
     * @param enqueuer постановщик задачи в очередь
     */
    public synchronized <T> void registerQueue(@Nonnull Queue<T> queue,
                                               @Nonnull Enqueuer<T> enqueuer) {
        Objects.requireNonNull(queue);
        Objects.requireNonNull(enqueuer);
        ensureConstructionInProgress();
        QueueLocation location = queue.getQueueConfig().getLocation();

        if (!Objects.equals(queue.getQueueConfig(), enqueuer.getQueueConfig())) {
            errorMessages.add(String.format("queue config must be the same: location=%s, enqueuer=%s, " +
                    "queue=%s", location, enqueuer.getQueueConfig(), queue.getQueueConfig()));
        }

        if (!Objects.equals(enqueuer.getPayloadTransformer(), queue.getPayloadTransformer())) {
            errorMessages.add(String.format("payload transformers must be the same: location=%s", location));
        }

        if (!Objects.equals(enqueuer.getShardRouter(), queue.getShardRouter())) {
            errorMessages.add(String.format("shard routers must be the same: location=%s", location));
        }

        if (queues.putIfAbsent(location, queue) != null) {
            errorMessages.add("duplicate queue: location=" + location);
        }
    }

    /**
     * Зарегистрировать шард БД
     *
     * @param queueDao dao для работы с шардом
     */
    public synchronized void registerShard(@Nonnull QueueDao queueDao) {
        Objects.requireNonNull(queueDao);
        ensureConstructionInProgress();
        QueueShardId shardId = queueDao.getShardId();
        if (shards.putIfAbsent(shardId, queueDao) != null) {
            errorMessages.add("duplicate shard: shardId=" + shardId);
        }
    }

    /**
     * Зарегистрировать слушатель задач заданной очереди
     *
     * @param location              идентификатор очереди
     * @param taskLifecycleListener слушатель задач
     */
    public synchronized void registerTaskLifecycleListener(
            @Nonnull QueueLocation location, @Nonnull TaskLifecycleListener taskLifecycleListener) {

        Objects.requireNonNull(location);
        Objects.requireNonNull(taskLifecycleListener);
        ensureConstructionInProgress();
        if (taskListeners.putIfAbsent(location, taskLifecycleListener) != null) {
            errorMessages.add("duplicate task lifecycle listener: location=" + location);
        }
    }

    /**
     * Зарегистрировать исполнителя задач для заданной очереди
     *
     * @param location         идентификатор очереди
     * @param externalExecutor исполнитель задач очереди
     */
    public synchronized void registerExternalExecutor(
            @Nonnull QueueLocation location, @Nonnull QueueExternalExecutor externalExecutor) {
        Objects.requireNonNull(location);
        Objects.requireNonNull(externalExecutor);
        ensureConstructionInProgress();
        if (externalExecutors.putIfAbsent(location, externalExecutor) != null) {
            errorMessages.add("duplicate external executor: location=" + location);
        }
    }

    /**
     * Завершить регистрацию конфигурации очередей
     */
    public synchronized void finishRegistration() {
        isRegistrationFinished = true;
        validateShards();
        validateTaskListeners();
        validateExternalExecutors();
        if (!errorMessages.isEmpty()) {
            throw new IllegalArgumentException("Invalid queue configuration:" + System.lineSeparator() +
                    errorMessages.stream().collect(Collectors.joining(System.lineSeparator())));
        }
        queues.values().forEach(queue -> log.info("registered queue: config={}", queue.getQueueConfig()));
        shards.values().forEach(shard -> log.info("registered shard: shardId={}", shard.getShardId()));
        externalExecutors.keySet().forEach(location -> log.info("registered external executor: location={}", location));
        taskListeners.keySet().forEach(location ->
                log.info("registered task lifecycle listener: location={}", location));

    }

    private void validateShards() {
        Map<QueueShardId, Boolean> shardsInUse = shards.keySet().stream()
                .collect(Collectors.toMap(id -> id, id -> Boolean.FALSE));
        for (ShardRouter shardRouter : queues.values().stream()
                .map(Queue::getShardRouter).collect(Collectors.toList())) {
            Collection<QueueShardId> routerShards = shardRouter.getShardsId();
            for (QueueShardId shardId : routerShards) {
                if (shards.containsKey(shardId)) {
                    shardsInUse.put(shardId, Boolean.TRUE);
                } else {
                    errorMessages.add("shard not found: shardId=" + shardId);
                }
            }
        }
        List<QueueShardId> unusedShards = shardsInUse.entrySet().stream()
                .filter(inUse -> !inUse.getValue()).map(Map.Entry::getKey).collect(Collectors.toList());
        if (!unusedShards.isEmpty()) {
            errorMessages.add("shards is not used: shardIds=" +
                    unusedShards.stream().map(QueueShardId::getId).collect(Collectors.joining(",")));
        }
    }

    private void validateTaskListeners() {
        for (QueueLocation location : taskListeners.keySet()) {
            if (!queues.containsKey(location)) {
                errorMessages.add("no matching queue for task listener: location=" + location);
            }
        }
    }

    private void validateExternalExecutors() {
        for (QueueLocation location : externalExecutors.keySet()) {
            if (!queues.containsKey(location)) {
                errorMessages.add("no matching queue for external executor: location=" + location);
            }
        }
        for (Map.Entry<QueueLocation, Queue> entry : queues.entrySet()) {
            boolean isUseExternalExecutor = entry.getValue().getQueueConfig()
                    .getSettings().getProcessingMode() == ProcessingMode.USE_EXTERNAL_EXECUTOR;
            boolean hasExternalExecutor = externalExecutors.containsKey(entry.getKey());
            if (isUseExternalExecutor && !hasExternalExecutor) {
                errorMessages.add("external executor missing " +
                        "for processing mode " + ProcessingMode.USE_EXTERNAL_EXECUTOR + ": location=" + entry.getKey());
            }
            if (!isUseExternalExecutor && hasExternalExecutor) {
                errorMessages.add("external executor must be specified only " +
                        "for processing mode " + ProcessingMode.USE_EXTERNAL_EXECUTOR + ": location=" + entry.getKey());
            }
        }

    }

    /**
     * Получить список зарегистрированных обработчиков очередей
     *
     * @return список очередей
     */
    @Nonnull
    Collection<Queue> getQueues() {
        ensureConstructionFinished();
        return Collections.unmodifiableCollection(queues.values());
    }

    /**
     * Получить зарегестрированные слушатели задач
     *
     * @return Map: key - местоположение очереди, value - слушатель данной очереди
     */
    @Nonnull
    Map<QueueLocation, TaskLifecycleListener> getTaskListeners() {
        ensureConstructionFinished();
        return Collections.unmodifiableMap(taskListeners);
    }

    /**
     * Получить исполнителей задач
     *
     * @return Map: key - местоположение очереди, value - исполнитель данной очереди
     */
    @Nonnull
    Map<QueueLocation, QueueExternalExecutor> getExternalExecutors() {
        ensureConstructionFinished();
        return Collections.unmodifiableMap(externalExecutors);
    }

    /**
     * Получить зарегестрированные шарды
     *
     * @return Map: key - идентификатор шарда, value - dao для работы с данным шардом
     */
    @Nonnull
    Map<QueueShardId, QueueDao> getShards() {
        ensureConstructionFinished();
        return Collections.unmodifiableMap(shards);
    }

    private void ensureConstructionFinished() {
        if (!isRegistrationFinished) {
            throw new IllegalStateException("cannot get registry property. construction is not finished");
        }
    }

    private void ensureConstructionInProgress() {
        if (isRegistrationFinished) {
            throw new IllegalStateException("cannot update property. construction is finished");
        }
    }


}
