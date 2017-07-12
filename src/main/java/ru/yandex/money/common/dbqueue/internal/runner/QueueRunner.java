package ru.yandex.money.common.dbqueue.internal.runner;

import ru.yandex.money.common.dbqueue.api.Queue;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.internal.MillisTimeProvider;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
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
     * @param queue очередь для обработки
     * @return таймаут ожидания после обработки задачи в очереди
     */
    @Nonnull
    Duration runQueue(@Nonnull Queue queue);

    /**
     * Фабрика исполнителей задач в очереди
     */
    final class Factory {

        private Factory() {
        }

        /**
         * Создать исполнителя задач очереди
         *
         * @param queue                 очередь обработки задач
         * @param queueDao              dao взаимодействия с очередью
         * @param taskLifecycleListener слушатель исполнения задач в очереди
         * @param externalExecutor      пул через который выполняются задачи в режиме
         *                              {@link ProcessingMode
         *                              #USE_EXTERNAL_EXECUTOR}
         * @return инстанс исполнителя задач
         */
        @SuppressWarnings("rawtypes")
        public static QueueRunner createQueueRunner(@Nonnull Queue queue, @Nonnull QueueDao queueDao,
                                                    @Nonnull TaskLifecycleListener taskLifecycleListener,
                                                    @Nullable Executor externalExecutor) {
            requireNonNull(queue);
            requireNonNull(queueDao);
            requireNonNull(taskLifecycleListener);

            PickTaskDao pickTaskDao = new PickTaskDao(queueDao.getShardId(),
                    queueDao.getJdbcTemplate(), queueDao.getTransactionTemplate());
            TaskPicker taskPicker = new TaskPicker(pickTaskDao, taskLifecycleListener,
                    new MillisTimeProvider.SystemMillisTimeProvider(), RetryTaskStrategy.Factory.create(
                    queue.getQueueConfig().getSettings()));
            TaskResultHandler taskResultHandler = new TaskResultHandler(queue.getQueueConfig().getLocation(), queueDao);
            TaskProcessor taskProcessor = new TaskProcessor(queueDao, taskLifecycleListener,
                    new MillisTimeProvider.SystemMillisTimeProvider(), taskResultHandler);
            QueueSettings settings = queue.getQueueConfig().getSettings();
            switch (settings.getProcessingMode()) {
                case SEPARATE_TRANSACTIONS:
                    return new QueueRunnerInSeparateTransactions(taskPicker, taskProcessor);
                case WRAP_IN_TRANSACTION:
                    return new QueueRunnerInTransaction(taskPicker, taskProcessor, queueDao);
                case USE_EXTERNAL_EXECUTOR:
                    return new QueueRunnerInExternalExecutor(taskPicker, taskProcessor,
                            requireNonNull(externalExecutor));
                default:
                    throw new IllegalStateException("unknown processing mode: " + settings.getProcessingMode());
            }
        }
    }
}
