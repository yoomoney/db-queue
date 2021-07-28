package ru.yoomoney.tech.dbqueue.api;

import javax.annotation.Nonnull;

/**
 * Task producer for the queue, which adds a new task into the queue.
 *
 * @param <PayloadT> The type of the payload in the task
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public interface QueueProducer<PayloadT> {

    /**
     * Add a new task into the queue
     *
     * @param enqueueParams Parameters with typed payload to enqueue the task
     * @return Enqueue result
     */
    EnqueueResult enqueue(@Nonnull EnqueueParams<PayloadT> enqueueParams);

    /**
     * Get task payload transformer, which transform the task's payload into the {@linkplain String}
     *
     * @return Task payload transformer
     */
    @Nonnull
    TaskPayloadTransformer<PayloadT> getPayloadTransformer();

}
