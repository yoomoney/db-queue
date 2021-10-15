package ru.yoomoney.tech.dbqueue.internal.runner;

import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;
import ru.yoomoney.tech.dbqueue.internal.processing.MillisTimeProvider;
import ru.yoomoney.tech.dbqueue.internal.processing.QueueProcessingStatus;
import ru.yoomoney.tech.dbqueue.internal.processing.TaskPicker;
import ru.yoomoney.tech.dbqueue.internal.processing.TaskProcessor;
import ru.yoomoney.tech.dbqueue.internal.processing.TaskResultHandler;
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

/**
 * Интерфейс обработчика пула задач очереди
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
@SuppressWarnings("rawtypes")
@FunctionalInterface
public interface QueueRunner {

    /**
     * Единократно обработать заданную очередь
     *
     * @param queueConsumer очередь для обработки
     * @return тип результата выполнения задачи
     */
    @Nonnull
    QueueProcessingStatus runQueue(@Nonnull QueueConsumer queueConsumer);

    /**
     * Фабрика исполнителей задач в очереди
     */
    final class Factory {

        private Factory() {
        }

        /**
         * Создать исполнителя задач очереди
         *
         * @param queueConsumer         очередь обработки задач
         * @param queueShard            шард, на котором будут запущен consumer
         * @param taskLifecycleListener слушатель процесса обработки задач
         * @return инстанс исполнителя задач
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        public static QueueRunner create(@Nonnull QueueConsumer queueConsumer,
                                         @Nonnull QueueShard<?> queueShard,
                                         @Nonnull TaskLifecycleListener taskLifecycleListener) {
            requireNonNull(queueConsumer);
            requireNonNull(queueShard);
            requireNonNull(taskLifecycleListener);

            QueueSettings queueSettings = queueConsumer.getQueueConfig().getSettings();
            QueueLocation queueLocation = queueConsumer.getQueueConfig().getLocation();

            QueuePickTaskDao queuePickTaskDao = queueShard.getDatabaseAccessLayer().createQueuePickTaskDao(
                    queueLocation,
                    queueSettings.getFailureSettings());

            TaskPicker taskPicker = new TaskPicker(queueShard, queueLocation, taskLifecycleListener,
                    new MillisTimeProvider.SystemMillisTimeProvider(), queuePickTaskDao);

            TaskResultHandler taskResultHandler = new TaskResultHandler(
                    queueLocation,
                    queueShard, queueSettings.getReenqueueSettings());

            TaskProcessor taskProcessor = new TaskProcessor(queueShard, taskLifecycleListener,
                    new MillisTimeProvider.SystemMillisTimeProvider(), taskResultHandler);

            ProcessingMode processingMode = queueSettings.getProcessingSettings().getProcessingMode();
            switch (processingMode) {
                case SEPARATE_TRANSACTIONS:
                    return new QueueRunnerInSeparateTransactions(taskPicker, taskProcessor);
                case WRAP_IN_TRANSACTION:
                    return new QueueRunnerInTransaction(taskPicker, taskProcessor, queueShard);
                case USE_EXTERNAL_EXECUTOR:
                    Optional<Executor> executor = queueConsumer.getExecutor();
                    return new QueueRunnerInExternalExecutor(taskPicker, taskProcessor,
                            executor.orElseThrow(() -> new IllegalArgumentException("Executor is empty. " +
                                    "You must provide QueueConsumer#getExecutor in ProcessingMode#USE_EXTERNAL_EXECUTOR")));
                default:
                    throw new IllegalStateException("unknown processing mode: " + processingMode);
            }
        }

    }
}
