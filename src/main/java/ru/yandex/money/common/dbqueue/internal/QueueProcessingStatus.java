package ru.yandex.money.common.dbqueue.internal;

/**
 * Тип результата обработки задачи в очереди
 *
 * @author Oleg Kandaurov
 * @since 27.08.2017
 */
public enum QueueProcessingStatus {

    /**
     * Задача была обрабатана
     */
    PROCESSED,
    /**
     * Задача не была найдена и обработка не состоялась
     */
    SKIPPED
}
