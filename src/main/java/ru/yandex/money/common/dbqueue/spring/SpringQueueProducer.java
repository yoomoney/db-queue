package ru.yandex.money.common.dbqueue.spring;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import ru.yandex.money.common.dbqueue.api.QueueProducer;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.QueueShardRouter;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
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
 * <p>
 * При окончании инициализации вызывается {@link #onInitialized()}
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
public abstract class SpringQueueProducer<T> implements QueueProducer<T>, SpringQueueIdentifiable {

    @Nonnull
    private final QueueLocation queueLocation;
    @Nonnull
    private final Class<T> payloadClass;
    private QueueConfig queueConfig;
    private TaskPayloadTransformer<T> payloadTransformer;
    private Map<QueueShardId, QueueDao> shards;
    private QueueShardRouter<T> shardRouter;

    /**
     * Конструктор постановщика задач
     *
     * @param queueLocation местоположение очереди
     * @param payloadClass  класс данных задачи
     */
    public SpringQueueProducer(@Nonnull QueueLocation queueLocation, @Nonnull Class<T> payloadClass) {
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
    public TaskPayloadTransformer<T> getPayloadTransformer() {
        return payloadTransformer;
    }

    @Nonnull
    @Override
    public QueueShardRouter<T> getShardRouter() {
        return shardRouter;
    }

    @Nonnull
    @Override
    public QueueConfig getQueueConfig() {
        return queueConfig;
    }

    /**
     * Установить конфигурацию очереди, к которой принадлежит текущий постановщик задач
     *
     * @param queueConfig конфигурация очереди
     */
    public void setQueueConfig(@Nonnull QueueConfig queueConfig) {
        this.queueConfig = Objects.requireNonNull(queueConfig);
    }

    /**
     * Установить преобразователь данных очереди
     *
     * @param payloadTransformer преобразователь данных
     */
    public void setPayloadTransformer(@Nonnull TaskPayloadTransformer<T> payloadTransformer) {
        this.payloadTransformer = Objects.requireNonNull(payloadTransformer);
    }

    /**
     * Установить список шардов, которые могут быть использованы при роутинге задачи на шард
     *
     * @param shards Map: key - идентификатор шарда, value - dao для работы с шардом
     */
    public void setShards(@Nonnull Map<QueueShardId, QueueDao> shards) {
        this.shards = Objects.requireNonNull(shards);
    }

    /**
     * Получить список шардов участвующих роутинге задач
     *
     * @return список шардов
     */
    @Nonnull
    public Collection<QueueDao> getShards() {
        return Collections.unmodifiableCollection(shards.values());
    }

    /**
     * Установить роутер для диспатчинг задач на шарды
     *
     * @param shardRouter роутер
     */
    public void setShardRouter(@Nonnull QueueShardRouter<T> shardRouter) {
        this.shardRouter = Objects.requireNonNull(shardRouter);
    }

    /**
     * Все поля постановщика задач очереди инициализированы.
     * Может быть использовано для валидации настроек.
     */
    @SuppressFBWarnings("ACEM_ABSTRACT_CLASS_EMPTY_METHODS")
    protected void onInitialized() {
    }

}
