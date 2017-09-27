package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;

/**
 * Интерфейс для определения принадлежности класса к очереди.
 *
 * @author Oleg Kandaurov
 * @since 20.07.2017
 */
@FunctionalInterface
interface SpringQueueIdentifiable {

    /**
     * Получить идентификатор очереди
     *
     * @return идентификатор очереди
     */
    @Nonnull
    QueueId getQueueId();

}
