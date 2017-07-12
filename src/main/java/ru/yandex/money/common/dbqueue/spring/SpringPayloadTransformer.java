package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.api.PayloadTransformer;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Преобразователь данных задачи, используемый в spring конфигурации.
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
public abstract class SpringPayloadTransformer<T> implements PayloadTransformer<T>, SpringQueueIdentifiable {

    @Nonnull
    private final QueueLocation queueLocation;
    @Nonnull
    private final Class<T> payloadClass;

    /**
     * Конструктор преобразователя данных задачи
     *
     * @param queueLocation местоположение очереди
     * @param payloadClass  класс данных задачи
     */
    protected SpringPayloadTransformer(@Nonnull QueueLocation queueLocation, @Nonnull Class<T> payloadClass) {
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

}
