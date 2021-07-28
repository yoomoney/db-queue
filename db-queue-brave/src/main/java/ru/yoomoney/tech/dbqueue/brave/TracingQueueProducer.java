package ru.yoomoney.tech.dbqueue.brave;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.EnqueueResult;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.api.TaskPayloadTransformer;
import ru.yoomoney.tech.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Wrapper for queue producer with tracing support via brave
 *
 * @param <PayloadT> The type of the payload in the task
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class TracingQueueProducer<PayloadT> implements QueueProducer<PayloadT> {

    private static final Logger log = LoggerFactory.getLogger(TracingQueueProducer.class);
    @Nonnull
    private final QueueProducer<PayloadT> queueProducer;
    @Nonnull
    private final Tracing tracing;
    @Nonnull
    private final B3SingleFormatSpanConverter spanConverter;
    @Nonnull
    private final QueueId queueId;
    @Nonnull
    private final String traceField;

    /**
     * Constructor
     *
     * @param queueProducer Original queue producer
     * @param queueId       Id of the Queue
     * @param tracing       Brave tracing object
     * @param traceField    Table field name for storing tracing info
     */
    public TracingQueueProducer(@Nonnull QueueProducer<PayloadT> queueProducer,
                                @Nonnull QueueId queueId,
                                @Nonnull Tracing tracing,
                                @Nonnull String traceField) {
        this.queueProducer = requireNonNull(queueProducer, "enqueuer");
        this.tracing = requireNonNull(tracing, "tracing");
        this.queueId = requireNonNull(queueId, "queueId");
        this.traceField = requireNonNull(traceField, "traceField");
        this.spanConverter = new B3SingleFormatSpanConverter(tracing);
    }

    private EnqueueResult enqueueInternal(@Nonnull EnqueueParams<PayloadT> enqueueParams, boolean isProduceNewTrace) {
        Span span = isProduceNewTrace ? tracing.tracer().newTrace() : tracing.tracer().nextSpan();
        if (isProduceNewTrace) {
            log.info("enqueing task in new trace: newTraceId={}", span.context().traceIdString());
        }
        span.name("qsend " + queueId.asString())
                .tag("queue.name", queueId.asString())
                .tag("queue.operation", "send")
                .kind(Span.Kind.PRODUCER);

        try (Tracer.SpanInScope spanInScope = tracing.tracer().withSpanInScope(span.start())) {
            enqueueParams.withExtData(traceField, spanConverter.serializeTraceContext(span.context()));
            return queueProducer.enqueue(enqueueParams);
        } finally {
            span.finish();
        }
    }

    @Override
    public EnqueueResult enqueue(@Nonnull EnqueueParams<PayloadT> enqueueParams) {
        return enqueueInternal(enqueueParams, false);
    }

    @Nonnull
    @Override
    public TaskPayloadTransformer<PayloadT> getPayloadTransformer() {
        return queueProducer.getPayloadTransformer();
    }

    /**
     * Add new task in a queue using new trace instead of inheriting existing trace.
     * <p>
     * Might be helpful in case of batch task processing.
     * Imagine you have a job which generate several tasks.
     * You can use this method to put every task in separate trace,
     * so you will be able to track tasks independently.
     *
     * @param enqueueParams Parameters with typed payload to enqueue the task
     * @return enqueue result
     */
    public EnqueueResult enqueueInNewTrace(@Nonnull EnqueueParams<PayloadT> enqueueParams) {
        return enqueueInternal(enqueueParams, true);
    }

}
