package ru.yandex.money.common.dbqueue.spring;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Класс обработки очереди, используемый в spring конфигурации
 * <p>
 * Класс не Immutable.
 * <p>
 * При окончании инициализации вызывается {@link #onInitialized()}
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
public abstract class SpringQueueConsumer<T> implements QueueConsumer<T>, SpringQueueIdentifiable {

    @Nonnull
    private final QueueId queueId;
    @Nonnull
    private final Class<T> payloadClass;
    private TaskPayloadTransformer<T> payloadTransformer;
    private QueueConfig queueConfig;
    private ConsumerShardsProvider shardRouter;

    /**
     * Конструктор очереди
     *
     * @param queueId идентификатор очереди
     * @param payloadClass  класс данных задачи
     */
    protected SpringQueueConsumer(@Nonnull QueueId queueId, @Nonnull Class<T> payloadClass) {
        this.queueId = Objects.requireNonNull(queueId);
        this.payloadClass = Objects.requireNonNull(payloadClass);
    }

    @Nonnull
    @Override
    public QueueId getQueueId() {
        return queueId;
    }

    @Nonnull
    @Override
    public QueueConfig getQueueConfig() {
        return queueConfig;
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<T> getPayloadTransformer() {
        return payloadTransformer;
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
     *
     * @param consumerShardsProvider хранитель шардов БД
     */
    void setConsumerShardsProvider(@Nonnull ConsumerShardsProvider consumerShardsProvider) {
        this.shardRouter = Objects.requireNonNull(consumerShardsProvider);
    }

    @Nonnull
    @Override
    public ConsumerShardsProvider getConsumerShardsProvider() {
        return shardRouter;
    }

    /**
     * Установить преобразователь данных очереди
     *
     * @param payloadTransformer преобразователь данных
     */
    void setPayloadTransformer(@Nonnull TaskPayloadTransformer<T> payloadTransformer) {
        this.payloadTransformer = Objects.requireNonNull(payloadTransformer);
    }

    /**
     * Установить конфигурацию очереди
     *
     * @param queueConfig конфигурация очереди
     */
    void setQueueConfig(@Nonnull QueueConfig queueConfig) {
        this.queueConfig = Objects.requireNonNull(queueConfig);
    }

    /**
     * Все поля обработчика очереди инициализированы.
     * Может быть использовано для валидации настроек.
     */
    @SuppressFBWarnings("ACEM_ABSTRACT_CLASS_EMPTY_METHODS")
    protected void onInitialized() {
    }

}
