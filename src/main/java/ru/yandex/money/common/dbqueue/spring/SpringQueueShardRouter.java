package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.api.QueueShardRouter;
import ru.yandex.money.common.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;

/**
 * Класс, описывающий правила шардирования задачи и исползуемый в spring конфигурации
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 30.07.2017
 */
public abstract class SpringQueueShardRouter<T> implements QueueShardRouter<T>, SpringQueueIdentifiable {

    private final QueueId queueId;
    private final Class<T> payloadClass;

    /**
     * Конструктор
     *
     * @param queueId идентификатор очереди
     * @param payloadClass  класс данных задачи
     */
    protected SpringQueueShardRouter(QueueId queueId, Class<T> payloadClass) {
        this.queueId = queueId;
        this.payloadClass = payloadClass;
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
