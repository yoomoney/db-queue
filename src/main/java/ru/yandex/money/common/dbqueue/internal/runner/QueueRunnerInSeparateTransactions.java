package ru.yandex.money.common.dbqueue.internal.runner;

import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.internal.processing.QueueProcessingStatus;
import ru.yandex.money.common.dbqueue.internal.processing.TaskPicker;
import ru.yandex.money.common.dbqueue.internal.processing.TaskProcessor;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;

import javax.annotation.Nonnull;

/**
 * Исполнитель задач очереди в режиме
 * {@link ProcessingMode#SEPARATE_TRANSACTIONS}
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class QueueRunnerInSeparateTransactions implements QueueRunner {

    private final BaseQueueRunner baseQueueRunner;

    /**
     * Конструктор
     *
     * @param taskPicker    выборщик задачи
     * @param taskProcessor обработчик задачи
     */
    QueueRunnerInSeparateTransactions(@Nonnull TaskPicker taskPicker,
                                      @Nonnull TaskProcessor taskProcessor) {
        baseQueueRunner = new BaseQueueRunner(taskPicker, taskProcessor, Runnable::run);
    }

    @Override
    @Nonnull
    public QueueProcessingStatus runQueue(@Nonnull QueueConsumer queueConsumer) {
        return baseQueueRunner.runQueue(queueConsumer);
    }

}
