package ru.yoomoney.tech.dbqueue.internal.processing;

import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueSettings;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Обработчик результат выполенения задачи
 *
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class TaskResultHandler {

    @Nonnull
    private final QueueLocation location;
    @Nonnull
    private final QueueShard<?> queueShard;
    @Nonnull
    private ReenqueueRetryStrategy reenqueueRetryStrategy;

    /**
     * Конструктор
     *
     * @param location          местоположение очереди
     * @param queueShard        шард на котором происходит обработка задачи
     * @param reenqueueSettings настройки переоткладывания задач
     */
    public TaskResultHandler(@Nonnull QueueLocation location,
                             @Nonnull QueueShard<?> queueShard,
                             @Nonnull ReenqueueSettings reenqueueSettings) {
        this.location = requireNonNull(location);
        this.queueShard = requireNonNull(queueShard);
        this.reenqueueRetryStrategy = ReenqueueRetryStrategy.Factory.create(reenqueueSettings);
        reenqueueSettings.registerObserver((oldValue, newValue) ->
                reenqueueRetryStrategy = ReenqueueRetryStrategy.Factory.create(newValue));
    }

    /**
     * Обработать результат выполнения задачи
     *
     * @param taskRecord      обработанная задача
     * @param executionResult результат обработки
     */
    public void handleResult(@Nonnull TaskRecord taskRecord, @Nonnull TaskExecutionResult executionResult) {
        requireNonNull(taskRecord);
        requireNonNull(executionResult);

        switch (executionResult.getActionType()) {
            case FINISH:
                queueShard.getDatabaseAccessLayer().transact(() -> queueShard.getDatabaseAccessLayer().getQueueDao()
                        .deleteTask(location, taskRecord.getId()));
                return;

            case REENQUEUE:
                queueShard.getDatabaseAccessLayer().transact(() -> queueShard.getDatabaseAccessLayer().getQueueDao()
                        .reenqueue(location, taskRecord.getId(),
                                executionResult.getExecutionDelay().orElseGet(
                                        () -> reenqueueRetryStrategy.calculateDelay(taskRecord))));
                return;
            case FAIL:
                return;

            default:
                throw new IllegalStateException("unknown action type: " + executionResult.getActionType());
        }
    }
}
