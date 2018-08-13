package ru.yandex.money.common.dbqueue.spring;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import ru.yandex.money.common.dbqueue.api.QueueProducer;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
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
    private final QueueId queueId;
    @Nonnull
    private final Class<T> payloadClass;
    private QueueConfig queueConfig;
    private TaskPayloadTransformer<T> payloadTransformer;
    private ProducerShardRouter<T> shardRouter;

    /**
     * Конструктор постановщика задач
     *
     * @param queueId идентификатор очереди
     * @param payloadClass  класс данных задачи
     */
    public SpringQueueProducer(@Nonnull QueueId queueId, @Nonnull Class<T> payloadClass) {
        this.queueId = Objects.requireNonNull(queueId);
        this.payloadClass = Objects.requireNonNull(payloadClass);
    }

    @Nonnull
    @Override
    public QueueId getQueueId() {
        return queueId;
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
    public ProducerShardRouter<T> getProducerShardRouter() {
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
     * Установить роутер для диспатчинга задач на шарды
     *
     * @param producerShardRouter роутер
     */
    public void setProducerShardRouter(@Nonnull ProducerShardRouter<T> producerShardRouter) {
        this.shardRouter = Objects.requireNonNull(producerShardRouter);
    }

    /**
     * Все поля постановщика задач очереди инициализированы.
     * Может быть использовано для валидации настроек.
     */
    @SuppressFBWarnings("ACEM_ABSTRACT_CLASS_EMPTY_METHODS")
    protected void onInitialized() {
    }

}
