package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.settings.QueueLocation;

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
     * Получить местоположение очереди
     *
     * @return местоположение очереди
     */
    @Nonnull
    QueueLocation getQueueLocation();

}
