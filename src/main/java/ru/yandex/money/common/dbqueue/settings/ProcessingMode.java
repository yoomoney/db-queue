package ru.yandex.money.common.dbqueue.settings;

import ru.yandex.money.common.dbqueue.api.QueueConsumer;

/**
 * Strategy for task processing in the queue.
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
public enum ProcessingMode {
    /**
     * Task will be processed at least once.
     * Each call to database will be done in separate transaction.
     * <p>
     * Should be used when there are external calls (HTTP etc.) in task processor.
     * However, external call <strong>must</strong> be idempotent.
     * In that mode the task processor might execute the task again
     * if after the successful task processing on client side it will be impossible
     * to delete the task from the queue.
     */
    SEPARATE_TRANSACTIONS,
    /**
     * Task processing wrapped into separate database transaction.
     * Task will be executed exactly once when all requirements met.
     * <p>
     * Should be used only when there are no external calls in task processor,
     * and the processor is only call the same database where the tasks are stored.
     * When all requirements are met, there is a guarantee that the task will be processed exactly once.
     * If there are external calls during task processing,
     * the database transaction might be kept open for a long period,
     * and the transaction pool will be exhausted.
     */
    WRAP_IN_TRANSACTION,

    /**
     * Task will be processed at least ones, asynchronously in given executor
     * {@link QueueConsumer#getExecutor()}.
     * Each call to database will be performed in separate transaction.
     * <p>
     * That mode requires an additional configuration and external executor management.
     * The benefit of this mode is a higher throughput
     * and capability to set upper limit for task processing speed.
     * This is achieved by the fact that the queue threads
     * are only picking tasks from the database,
     * whilst subsequent task processing is carried out in separate executor.
     * <p>
     * This mode should be used when the queue performs long-running operations.
     * If this mode will not be used, then the problem might be solved
     * with increasing the number of queue processing threads,
     * although this also will lead to the increasing database idle polls.
     */
    USE_EXTERNAL_EXECUTOR
}
