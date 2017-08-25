package ru.yandex.money.common.dbqueue.internal.runner;

import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Обработчик результат выполенения задачи
 *
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
class TaskResultHandler {

    @Nonnull
    private final QueueLocation location;
    @Nonnull
    private final QueueDao queueDao;

    /**
     * Конструктор
     *
     * @param location местоположение очереди
     * @param queueDao шард на котором происходит обработка задачи
     */
    TaskResultHandler(@Nonnull QueueLocation location, @Nonnull QueueDao queueDao) {
        this.location = Objects.requireNonNull(location);
        this.queueDao = Objects.requireNonNull(queueDao);
    }

    /**
     * Обработать результат выполнения задачи
     *
     * @param taskRecord      обработанная задача
     * @param executionResult результат обработки
     */
    void handleResult(@Nonnull TaskRecord taskRecord, @Nonnull TaskExecutionResult executionResult) {
        Objects.requireNonNull(taskRecord);
        Objects.requireNonNull(executionResult);
        switch (executionResult.getActionType()) {
            case FINISH:
                queueDao.getTransactionTemplate().execute(status ->
                        queueDao.deleteTask(location, taskRecord.getId()));
                return;

            case REENQUEUE:
                queueDao.getTransactionTemplate().execute(
                        status -> queueDao.reenqueue(location, taskRecord.getId(),
                                executionResult.getExecutionDelayOrThrow(), true));
                return;
            case FAIL:
                executionResult.getExecutionDelay().ifPresent(delay -> queueDao.getTransactionTemplate()
                        .execute(status -> queueDao.reenqueue(location, taskRecord.getId(), delay, false)));
                return;
            default:
                throw new IllegalStateException("unknown action type: " + executionResult.getActionType());
        }
    }
}
