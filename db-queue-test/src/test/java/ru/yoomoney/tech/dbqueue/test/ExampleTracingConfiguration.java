package ru.yoomoney.tech.dbqueue.test;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.context.log4j2.ThreadContextScopeDecorator;
import brave.propagation.ThreadLocalCurrentTraceContext;
import org.junit.Assert;
import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.api.impl.MonitoringQueueProducer;
import ru.yoomoney.tech.dbqueue.api.impl.NoopPayloadTransformer;
import ru.yoomoney.tech.dbqueue.api.impl.ShardingQueueProducer;
import ru.yoomoney.tech.dbqueue.api.impl.SingleQueueShardRouter;
import ru.yoomoney.tech.dbqueue.brave.TracingQueueProducer;
import ru.yoomoney.tech.dbqueue.brave.TracingTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.config.QueueService;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.config.impl.CompositeTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.config.impl.CompositeThreadLifecycleListener;
import ru.yoomoney.tech.dbqueue.config.impl.LoggingTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.config.impl.LoggingThreadLifecycleListener;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.spring.dao.SpringDatabaseAccessLayer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class ExampleTracingConfiguration {

    public static final String PG_TRACING_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id                BIGSERIAL PRIMARY KEY,\n" +
            "  queue_name        TEXT NOT NULL,\n" +
            "  payload           TEXT,\n" +
            "  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  attempt           INTEGER                  DEFAULT 0,\n" +
            "  reenqueue_attempt INTEGER                  DEFAULT 0,\n" +
            "  total_attempt     INTEGER                  DEFAULT 0,\n" +
            "  trace_info        TEXT\n" +
            ");" +
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (queue_name, next_process_at, id DESC);\n" +
            "\n";

    @Test
    public void tracing_config() throws InterruptedException {
        AtomicInteger taskConsumedCount = new AtomicInteger(0);
        DefaultDatabaseInitializer.createTable(PG_TRACING_TABLE_DDL, "tracing_task_table");
        SpringDatabaseAccessLayer databaseAccessLayer = new SpringDatabaseAccessLayer(
                DatabaseDialect.POSTGRESQL, QueueTableSchema.builder()
                .withExtFields(Collections.singletonList("trace_info")).build(),
                DefaultDatabaseInitializer.getJdbcTemplate(),
                DefaultDatabaseInitializer.getTransactionTemplate());
        QueueShard<SpringDatabaseAccessLayer> shard = new QueueShard<>(new QueueShardId("main"), databaseAccessLayer);

        QueueId queueId = new QueueId("tracing_queue");
        QueueConfig config = new QueueConfig(QueueLocation.builder().withTableName("tracing_task_table")
                .withQueueId(queueId).build(),
                QueueSettings.builder()
                        .withBetweenTaskTimeout(Duration.ofMillis(100))
                        .withNoTaskTimeout(Duration.ofMillis(100))
                        .build());

        Tracing tracing = Tracing.newBuilder().currentTraceContext(ThreadLocalCurrentTraceContext.newBuilder()
                .addScopeDecorator(ThreadContextScopeDecorator.create())
                .build()).build();

        ShardingQueueProducer<String, SpringDatabaseAccessLayer> shardingQueueProducer = new ShardingQueueProducer<>(
                config, NoopPayloadTransformer.getInstance(), new SingleQueueShardRouter<>(shard));
        QueueProducer<String> monitoringQueueProducer = new MonitoringQueueProducer<>(shardingQueueProducer, queueId);
        TracingQueueProducer<String> tracingQueueProducer = new TracingQueueProducer<>(monitoringQueueProducer, queueId, tracing, "trace_info");
        StringQueueConsumer consumer = new StringQueueConsumer(config, taskConsumedCount);

        QueueService queueService = new QueueService(singletonList(shard),
                new CompositeThreadLifecycleListener(singletonList(
                        new LoggingThreadLifecycleListener())),
                new CompositeTaskLifecycleListener(Arrays.asList(
                        new TracingTaskLifecycleListener(tracing, "trace_info"),
                        new LoggingTaskLifecycleListener())));

        queueService.registerQueue(consumer);
        queueService.start();
        Span span = tracing.tracer().newTrace();
        try (Tracer.SpanInScope spanInScope = tracing.tracer().withSpanInScope(span)) {
            tracingQueueProducer.enqueue(EnqueueParams.create("tracing task"));
        }
        sleep(1000);
        queueService.shutdown();
        queueService.awaitTermination(Duration.ofSeconds(10));
        Assert.assertThat(taskConsumedCount.get(), equalTo(1));
    }

}
