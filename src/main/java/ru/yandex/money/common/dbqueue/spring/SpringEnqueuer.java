package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.ShardRouter;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.api.Enqueuer;
import ru.yandex.money.common.dbqueue.api.PayloadTransformer;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Постановщик задач в очередь используемый в spring конфигурации.
 * <p>
 * Класс не Immutable.
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
public abstract class SpringEnqueuer<T> implements Enqueuer<T>, SpringQueueIdentifiable {

    @Nonnull
    private final QueueLocation queueLocation;
    @Nonnull
    private final Class<T> payloadClass;
    private QueueConfig queueConfig;
    private PayloadTransformer<T> payloadTransformer;
    private Map<QueueShardId, QueueDao> shards;
    private ShardRouter<T> shardRouter;

    /**
     * Конструктор постановщика задач
     *
     * @param queueLocation местоположение очереди
     * @param payloadClass  класс данных задачи
     */
    public SpringEnqueuer(@Nonnull QueueLocation queueLocation, @Nonnull Class<T> payloadClass) {
        this.queueLocation = Objects.requireNonNull(queueLocation);
        this.payloadClass = Objects.requireNonNull(payloadClass);
    }

    @Nonnull
    @Override
    public QueueLocation getQueueLocation() {
        return queueLocation;
    }

    /**
     * Получить класс данных задачи в очереди
     *
     * @return класс данных задачи
     */
    @Nonnull
    Class<T> getPayloadClass() {
        return payloadClass;
    }

    @Nonnull
    @Override
    public PayloadTransformer<T> getPayloadTransformer() {
        return payloadTransformer;
    }

    @Nonnull
    @Override
    public ShardRouter<T> getShardRouter() {
        return shardRouter;
    }

    @Nonnull
    @Override
    public QueueConfig getQueueConfig() {
        return queueConfig;
    }

    /**
     * Установить конфигурацию очереди, к которой принадлежит текущий постановщик задач
     * @param queueConfig конфигурация очереди
     */
    void setQueueConfig(@Nonnull QueueConfig queueConfig) {
        this.queueConfig = Objects.requireNonNull(queueConfig);
    }

    /**
     * Установить преобразователь данных очереди
     * @param payloadTransformer преобразователь данных
     */
    void setPayloadTransformer(@Nonnull PayloadTransformer<T> payloadTransformer) {
        this.payloadTransformer = Objects.requireNonNull(payloadTransformer);
    }

    /**
     * Установить список шардов, которые могут быть использованы при роутинге задачи на шард
     * @param shards Map: key - идентификатор шарда, value - dao для работы с шардом
     */
    void setShards(@Nonnull Map<QueueShardId, QueueDao> shards) {
        this.shards = Objects.requireNonNull(shards);
    }

    /**
     * Получить список шардов участвующих роутинге задач
     * @return список шардов
     */
    @Nonnull
    public Collection<QueueDao> getShards() {
        return Collections.unmodifiableCollection(shards.values());
    }

    /**
     * Установить роутер для диспатчинг задач на шарды
     * @param shardRouter роутер
     */
    void setShardRouter(@Nonnull ShardRouter<T> shardRouter) {
        this.shardRouter = Objects.requireNonNull(shardRouter);
    }

}
