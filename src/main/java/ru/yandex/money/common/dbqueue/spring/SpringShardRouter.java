package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.api.ShardRouter;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;

/**
 * Класс, описывающий правила шардирования задачи и исползуемый в spring конфигурации
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 30.07.2017
 */
public abstract class SpringShardRouter<T> implements ShardRouter<T>, SpringQueueIdentifiable {

    private final QueueLocation queueLocation;
    private final Class<T> payloadClass;

    /**
     * Конструктор
     *
     * @param queueLocation местоположение очереди
     * @param payloadClass  класс данных задачи
     */
    protected SpringShardRouter(QueueLocation queueLocation, Class<T> payloadClass) {
        this.queueLocation = queueLocation;
        this.payloadClass = payloadClass;
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
