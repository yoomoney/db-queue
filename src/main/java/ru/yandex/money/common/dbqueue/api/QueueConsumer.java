package ru.yandex.money.common.dbqueue.api;

import ru.yandex.money.common.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Обработчик задач в очереди.
 *
 * @param <T> Тип данных задачи в очереди
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public interface QueueConsumer<T> {

    /**
     * Обработать задачу.
     *
     * @param task задача на обработку
     * @return результат выполнения очереди
     */
    @Nonnull
    TaskExecutionResult execute(@Nonnull Task<T> task);

    /**
     * Получить конфигурацию данной очереди.
     *
     * @return конфигурация очереди
     */
    @Nonnull
    QueueConfig getQueueConfig();

    /**
     * Предоставить преобразователь данных задачи.
     *
     * @return преобразователь данных
     */
    @Nonnull
    TaskPayloadTransformer<T> getPayloadTransformer();

    /**
     * Получить инстанс класс, представляющий шарды для обработки задачи
     *
     * @return шарды БД
     */
    @Nonnull
    ConsumerShardsProvider getConsumerShardsProvider();

    /**
     * Предоставление шардов БД для обработки задачи
     *
     * @author Oleg Kandaurov
     * @since 13.08.2018
     */
    interface ConsumerShardsProvider {

        /**
         * Получить список всех шардов, на которых обрабатывается задача.
         *
         * @return идентификаторы шардов
         */
        @Nonnull
        Collection<QueueShard> getProcessingShards();
    }
}
