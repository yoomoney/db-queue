package ru.yandex.money.common.dbqueue.internal.runner;

import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueShard;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.internal.MillisTimeProvider;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Обработчик выбранной задачи
 *
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class TaskProcessor {

    @Nonnull
    private final QueueShard queueShard;
    @Nonnull
    private final TaskLifecycleListener taskLifecycleListener;
    @Nonnull
    private final MillisTimeProvider millisTimeProvider;
    @Nonnull
    private final TaskResultHandler taskResultHandler;

    /**
     * Конструктор
     *
     * @param queueShard            шард на котором происходит выполнение задачи
     * @param taskLifecycleListener слушатель жизненного цикла задачи в очереди
     * @param millisTimeProvider    поставщик текущего времени
     * @param taskResultHandler     обработчик результата выполнения задачи
     */
    TaskProcessor(@Nonnull QueueShard queueShard,
                  @Nonnull TaskLifecycleListener taskLifecycleListener,
                  @Nonnull MillisTimeProvider millisTimeProvider,
                  @Nonnull TaskResultHandler taskResultHandler) {
        this.queueShard = requireNonNull(queueShard);
        this.taskLifecycleListener = requireNonNull(taskLifecycleListener);
        this.millisTimeProvider = requireNonNull(millisTimeProvider);
        this.taskResultHandler = requireNonNull(taskResultHandler);
    }

    /**
     * Передать выбранную задачу в клиентский код на выполнение и обработать результат
     *
     * @param queueConsumer очередь
     * @param taskRecord    запись на обработку
     */
    void processTask(@Nonnull QueueConsumer queueConsumer, @Nonnull TaskRecord taskRecord) {
        requireNonNull(queueConsumer);
        requireNonNull(taskRecord);
        try {
            taskLifecycleListener.started(queueShard.getShardId(), queueConsumer.getQueueConfig().getLocation(),
                    taskRecord);
            long processTaskStarted = millisTimeProvider.getMillis();
            Object payload = queueConsumer.getPayloadTransformer().toObject(taskRecord.getPayload());
            Task task = new Task(queueShard.getShardId(), payload, taskRecord.getAttemptsCount(),
                    taskRecord.getReenqueueAttemptsCount(), taskRecord.getTotalAttemptsCount(), taskRecord.getCreateDate(),
                    taskRecord.getTraceInfo(), taskRecord.getActor());
            TaskExecutionResult executionResult = queueConsumer.execute(task);
            taskLifecycleListener.executed(queueShard.getShardId(), queueConsumer.getQueueConfig().getLocation(),
                    taskRecord,
                    executionResult, millisTimeProvider.getMillis() - processTaskStarted);
            taskResultHandler.handleResult(taskRecord, executionResult);
        } catch (Exception exc) {
            taskLifecycleListener.crashed(queueShard.getShardId(), queueConsumer.getQueueConfig().getLocation(),
                    taskRecord, exc);
        } finally {
            taskLifecycleListener.finished(queueShard.getShardId(), queueConsumer.getQueueConfig().getLocation(),
                    taskRecord);
        }
    }

}
