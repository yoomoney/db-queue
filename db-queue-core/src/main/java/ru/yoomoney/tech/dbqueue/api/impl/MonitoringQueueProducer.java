package ru.yoomoney.tech.dbqueue.api.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.EnqueueResult;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.api.TaskPayloadTransformer;
import ru.yoomoney.tech.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Wrapper for queue producer with logging and monitoring support
 *
 * @param <PayloadT> The type of the payload in the task
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class MonitoringQueueProducer<PayloadT> implements QueueProducer<PayloadT> {

    private static final Logger log = LoggerFactory.getLogger(MonitoringQueueProducer.class);

    @Nonnull
    private final QueueProducer<PayloadT> queueProducer;
    @Nonnull
    private final QueueId queueId;
    @Nonnull
    private final BiConsumer<EnqueueResult, Long> monitoringCallback;
    @Nonnull
    private final Clock clock;

    /**
     * Constructor
     *
     * @param queueProducer      Task producer for the queue
     * @param queueId            Id of the queue
     * @param monitoringCallback Callback invoked after putting a task in the queue.
     *                           It might help to monitor enqueue time.
     * @param clock              A clock to mock current time
     */
    MonitoringQueueProducer(@Nonnull QueueProducer<PayloadT> queueProducer,
                            @Nonnull QueueId queueId,
                            @Nonnull BiConsumer<EnqueueResult, Long> monitoringCallback,
                            @Nonnull Clock clock) {
        this.queueProducer = Objects.requireNonNull(queueProducer);
        this.queueId = Objects.requireNonNull(queueId);
        this.monitoringCallback = Objects.requireNonNull(monitoringCallback);
        this.clock = Objects.requireNonNull(clock);
    }

    /**
     * Constructor
     *
     * @param queueProducer      Task producer for the queue
     * @param queueId            Id of the queue
     * @param monitoringCallback Callback invoked after putting a task in the queue.
     *                           It might help to monitor enqueue time.
     */
    public MonitoringQueueProducer(@Nonnull QueueProducer<PayloadT> queueProducer,
                                   @Nonnull QueueId queueId,
                                   @Nonnull BiConsumer<EnqueueResult, Long> monitoringCallback) {
        this(queueProducer, queueId, monitoringCallback, Clock.systemDefaultZone());
    }

    /**
     * Constructor
     *
     * @param queueProducer Task producer for the queue
     * @param queueId       Id of the queue
     */
    public MonitoringQueueProducer(@Nonnull QueueProducer<PayloadT> queueProducer,
                                   @Nonnull QueueId queueId) {
        this(queueProducer, queueId, ((enqueueResult, id) -> {
        }));
    }

    @Override
    public EnqueueResult enqueue(@Nonnull EnqueueParams<PayloadT> enqueueParams) {
        log.info("enqueuing task: queue={}, delay={}", queueId, enqueueParams.getExecutionDelay());
        long startTime = clock.millis();
        EnqueueResult enqueueResult = queueProducer.enqueue(enqueueParams);
        log.info("task enqueued: id={}, queueShardId={}", enqueueResult.getEnqueueId(), enqueueResult.getShardId());
        long elapsedTime = clock.millis() - startTime;
        monitoringCallback.accept(enqueueResult, elapsedTime);
        return enqueueResult;
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<PayloadT> getPayloadTransformer() {
        return queueProducer.getPayloadTransformer();
    }
}
