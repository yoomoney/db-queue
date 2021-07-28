package ru.yoomoney.tech.dbqueue.api;

import ru.yoomoney.tech.dbqueue.settings.ProcessingMode;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.Executor;

/**
 * Task processor for the queue
 *
 * @param <PayloadT> The type of the payload in the task
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public interface QueueConsumer<PayloadT> {

    /**
     * Process the task from the queue
     *
     * @param task A typed task for processing
     * @return A result of task processing
     */
    @Nonnull
    TaskExecutionResult execute(@Nonnull Task<PayloadT> task);

    /**
     * Get queue configuration
     *
     * @return Queue configuration
     */
    @Nonnull
    QueueConfig getQueueConfig();

    /**
     * Get task payload transformer, which transform the task's {@linkplain String} payload into the type of the task
     *
     * @return Task payload transformer
     */
    @Nonnull
    TaskPayloadTransformer<PayloadT> getPayloadTransformer();

    /**
     * Task executor for {@link ProcessingMode#USE_EXTERNAL_EXECUTOR} mode.
     * Applies only to that mode
     *
     * @return {@linkplain Optional} of external task executor
     */
    default Optional<Executor> getExecutor() {
        return Optional.empty();
    }

}
