package ru.yandex.money.common.dbqueue.api;

import java.util.Collection;

/**
 * Интерфейс, предоставляющий правила шардирования задачи.
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 29.07.2017
 */
public interface ShardRouter<T> {

    /**
     * Получить шард, на котором должна быть размещена задача
     *
     * @param enqueueParams данные постновки задачи в очередь
     * @return идентификатор шарда
     */
    QueueShardId resolveShardId(EnqueueParams<T> enqueueParams);

    /**
     * Получить список всех шардов, на которые может быть помещена задача.
     *
     * @return идентификаторы шардов
     */
    Collection<QueueShardId> getShardsId();

}
