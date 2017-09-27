package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Преобразователь данных задачи, используемый в spring конфигурации.
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
public abstract class SpringTaskPayloadTransformer<T> implements TaskPayloadTransformer<T>, SpringQueueIdentifiable {

    @Nonnull
    private final QueueId queueId;
    @Nonnull
    private final Class<T> payloadClass;

    /**
     * Конструктор преобразователя данных задачи
     *
     * @param queueId идентификатор очереди
     * @param payloadClass  класс данных задачи
     */
    protected SpringTaskPayloadTransformer(@Nonnull QueueId queueId, @Nonnull Class<T> payloadClass) {
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

}
