package ru.yandex.money.common.dbqueue.internal.runner;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.config.QueueShard;
import ru.yandex.money.common.dbqueue.internal.processing.QueueProcessingStatus;
import ru.yandex.money.common.dbqueue.internal.processing.TaskPicker;
import ru.yandex.money.common.dbqueue.internal.processing.TaskProcessor;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

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
    QueueRunnerInTransaction(@Nonnull TaskPicker taskPicker,
                             @Nonnull TaskProcessor taskProcessor,
                             @Nonnull QueueShard queueShard) {
        this.queueShard = requireNonNull(queueShard);
        baseQueueRunner = new BaseQueueRunner(taskPicker, taskProcessor, Runnable::run);
    }

    @Override
    @Nonnull
    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public QueueProcessingStatus runQueue(@Nonnull QueueConsumer queueConsumer) {
        return requireNonNull(queueShard.getTransactionTemplate().execute(status ->
                baseQueueRunner.runQueue(queueConsumer)));
    }
}