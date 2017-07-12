package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.api.PayloadTransformer;
import ru.yandex.money.common.dbqueue.api.Queue;
import ru.yandex.money.common.dbqueue.api.ShardRouter;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Класс обработки очереди, используемый в spring конфигурации
 * Класс не Immutable.
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
public abstract class SpringQueue<T> implements Queue<T>, SpringQueueIdentifiable {

    @Nonnull
    private final QueueLocation queueLocation;
    @Nonnull
    private final Class<T> payloadClass;
    private PayloadTransformer<T> payloadTransformer;
    private QueueConfig queueConfig;
    private ShardRouter<T> shardRouter;

    /**
     * Конструктор очереди
     *
     * @param queueLocation местоположение очереди
     * @param payloadClass  класс данных задачи
     */
    protected SpringQueue(@Nonnull QueueLocation queueLocation, @Nonnull Class<T> payloadClass) {
        this.queueLocation = Objects.requireNonNull(queueLocation);
        this.payloadClass = Objects.requireNonNull(payloadClass);
    }

    @Nonnull
    @Override
    public QueueLocation getQueueLocation() {
        return queueLocation;
    }

    @Nonnull
    @Override
    public QueueConfig getQueueConfig() {
        return queueConfig;
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

    /**
     * Получить класс данных задачи в очереди
     *
     * @return класс данных задачи
     */
    @Nonnull
    Class<T> getPayloadClass() {
        return payloadClass;
    }

    /**
     * Установить роутер для диспатчинг задач на шарды
     * @param shardRouter роутер
     */
    void setShardRouter(@Nonnull ShardRouter<T> shardRouter) {
        this.shardRouter = Objects.requireNonNull(shardRouter);
    }

    /**
     * Установить преобразователь данных очереди
     * @param payloadTransformer преобразователь данных
     */
    void setPayloadTransformer(@Nonnull PayloadTransformer<T> payloadTransformer) {
        this.payloadTransformer = Objects.requireNonNull(payloadTransformer);
    }

    /**
     * Установить конфигурацию очереди
     * @param queueConfig конфигурация очереди
     */
    void setQueueConfig(@Nonnull QueueConfig queueConfig) {
        this.queueConfig = Objects.requireNonNull(queueConfig);
    }

}
