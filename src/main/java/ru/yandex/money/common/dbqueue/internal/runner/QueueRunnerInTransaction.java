package ru.yandex.money.common.dbqueue.internal.runner;

import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueShard;
import ru.yandex.money.common.dbqueue.internal.QueueProcessingStatus;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Исполнитель задач очереди в режиме
 * {@link ProcessingMode#WRAP_IN_TRANSACTION}
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
@SuppressWarnings("rawtypes")
class QueueRunnerInTransaction implements QueueRunner {

    @Nonnull
    private final QueueShard queueShard;
    private final BaseQueueRunner baseQueueRunner;

    /**
     * Конструктор
     *
     * @param taskPicker    выборщик задачи
     * @param taskProcessor обработчик задачи
     * @param queueShard    шард на котором обрабатываются задачи
     */
    QueueRunnerInTransaction(@Nonnull TaskPicker taskPicker, @Nonnull TaskProcessor taskProcessor,
                             @Nonnull QueueShard queueShard) {
        this.queueShard = Objects.requireNonNull(queueShard);
        baseQueueRunner = new BaseQueueRunner(taskPicker, taskProcessor, Runnable::run);
    }

    @Override
    @Nonnull
    public QueueProcessingStatus runQueue(@Nonnull QueueConsumer queueConsumer) {
        return queueShard.getTransactionTemplate().execute(status ->
                baseQueueRunner.runQueue(queueConsumer));
    }
}