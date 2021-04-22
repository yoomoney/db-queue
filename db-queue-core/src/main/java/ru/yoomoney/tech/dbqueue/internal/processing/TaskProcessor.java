package ru.yoomoney.tech.dbqueue.internal.processing;

import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.api.Task;
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Обработчик выбранной задачи
 *
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TaskProcessor {

    @Nonnull
    private final QueueShard<?> queueShard;
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
    public TaskProcessor(@Nonnull QueueShard<?> queueShard,
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
    public void processTask(@Nonnull QueueConsumer queueConsumer, @Nonnull TaskRecord taskRecord) {
        requireNonNull(queueConsumer);
        requireNonNull(taskRecord);
        try {
            taskLifecycleListener.started(queueShard.getShardId(), queueConsumer.getQueueConfig().getLocation(),
                    taskRecord);
            long processTaskStarted = millisTimeProvider.getMillis();
            Object payload = queueConsumer.getPayloadTransformer().toObject(taskRecord.getPayload());
            Task<?> task = Task.builder(queueShard.getShardId())
                    .withCreatedAt(taskRecord.getCreatedAt())
                    .withPayload(payload)
                    .withAttemptsCount(taskRecord.getAttemptsCount())
                    .withReenqueueAttemptsCount(taskRecord.getReenqueueAttemptsCount())
                    .withTotalAttemptsCount(taskRecord.getTotalAttemptsCount())
                    .withExtData(taskRecord.getExtData())
                    .build();
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
