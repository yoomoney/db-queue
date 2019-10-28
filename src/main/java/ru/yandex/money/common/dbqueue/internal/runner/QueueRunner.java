package ru.yandex.money.common.dbqueue.internal.runner;

import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.config.QueueShard;
import ru.yandex.money.common.dbqueue.config.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.internal.pick.PickTaskSettings;
import ru.yandex.money.common.dbqueue.internal.processing.MillisTimeProvider;
import ru.yandex.money.common.dbqueue.internal.processing.QueueProcessingStatus;
import ru.yandex.money.common.dbqueue.internal.processing.ReenqueueRetryStrategy;
import ru.yandex.money.common.dbqueue.internal.processing.TaskPicker;
import ru.yandex.money.common.dbqueue.internal.processing.TaskProcessor;
import ru.yandex.money.common.dbqueue.internal.processing.TaskResultHandler;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;

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
                                         @Nonnull QueueShard queueShard,
                                         @Nonnull TaskLifecycleListener taskLifecycleListener) {
            requireNonNull(queueConsumer);
            requireNonNull(queueShard);
            requireNonNull(taskLifecycleListener);

            QueueSettings queueSettings = queueConsumer.getQueueConfig().getSettings();

            ReenqueueRetryStrategy reenqueueRetryStrategy = ReenqueueRetryStrategy.Factory
                    .create(queueSettings.getReenqueueRetrySettings());

            TaskPicker taskPicker = new TaskPicker(queueShard, taskLifecycleListener,
                    new MillisTimeProvider.SystemMillisTimeProvider(),
                    new PickTaskSettings(
                            queueSettings.getRetryType(),
                            queueSettings.getRetryInterval()));

            TaskResultHandler taskResultHandler = new TaskResultHandler(
                    queueConsumer.getQueueConfig().getLocation(),
                    queueShard, reenqueueRetryStrategy);

            TaskProcessor taskProcessor = new TaskProcessor(queueShard, taskLifecycleListener,
                    new MillisTimeProvider.SystemMillisTimeProvider(), taskResultHandler);

            ProcessingMode processingMode = queueSettings.getProcessingMode();
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
