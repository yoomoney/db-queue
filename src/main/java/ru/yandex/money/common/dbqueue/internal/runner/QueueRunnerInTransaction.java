package ru.yandex.money.common.dbqueue.internal.runner;

import ru.yandex.money.common.dbqueue.api.Queue;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.time.Duration;
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
    private final QueueDao queueDao;
    @Nonnull
    private final TaskPicker taskPicker;
    @Nonnull
    private final TaskProcessor taskProcessor;

    /**
     * Конструктор
     *
     * @param taskPicker выборщик задачи
     * @param taskProcessor обработчик задачи
     * @param queueDao шард на котором обрабатываются задачи
     */
    QueueRunnerInTransaction(@Nonnull TaskPicker taskPicker, @Nonnull TaskProcessor taskProcessor,
                             @Nonnull QueueDao queueDao) {
        this.queueDao = Objects.requireNonNull(queueDao);
        this.taskPicker = Objects.requireNonNull(taskPicker);
        this.taskProcessor = Objects.requireNonNull(taskProcessor);
    }

    @Override
    @Nonnull
    public Duration runQueue(@Nonnull Queue queue) {
        return queueDao.getTransactionTemplate().execute(status -> {
            QueueConfig config = queue.getQueueConfig();
            TaskRecord taskRecord = taskPicker.pickTask(queue);
            if (taskRecord == null) {
                return config.getSettings().getNoTaskTimeout();
            }
            taskProcessor.processTask(queue, taskRecord);
            return config.getSettings().getBetweenTaskTimeout();
        });
    }
}