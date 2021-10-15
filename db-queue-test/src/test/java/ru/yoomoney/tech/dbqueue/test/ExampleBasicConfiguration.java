package ru.yoomoney.tech.dbqueue.test;

import org.junit.Assert;
import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.api.impl.MonitoringQueueProducer;
import ru.yoomoney.tech.dbqueue.api.impl.NoopPayloadTransformer;
import ru.yoomoney.tech.dbqueue.api.impl.ShardingQueueProducer;
import ru.yoomoney.tech.dbqueue.api.impl.SingleQueueShardRouter;
import ru.yoomoney.tech.dbqueue.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.config.QueueService;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.config.impl.LoggingTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.config.impl.LoggingThreadLifecycleListener;
import ru.yoomoney.tech.dbqueue.settings.ExtSettings;
import ru.yoomoney.tech.dbqueue.settings.FailRetryType;
import ru.yoomoney.tech.dbqueue.settings.FailureSettings;
import ru.yoomoney.tech.dbqueue.settings.PollSettings;
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode;
import ru.yoomoney.tech.dbqueue.settings.ProcessingSettings;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueRetryType;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueSettings;
import ru.yoomoney.tech.dbqueue.spring.dao.SpringDatabaseAccessLayer;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * @author Oleg Kandaurov
 * @since 14.08.2017
 */
public class ExampleBasicConfiguration {

    public static final String PG_DEFAULT_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id                BIGSERIAL PRIMARY KEY,\n" +
            "  queue_name        TEXT NOT NULL,\n" +
            "  payload           TEXT,\n" +
            "  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  attempt           INTEGER                  DEFAULT 0,\n" +
            "  reenqueue_attempt INTEGER                  DEFAULT 0,\n" +
            "  total_attempt     INTEGER                  DEFAULT 0\n" +
            ");" +
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (queue_name, next_process_at, id DESC);\n" +
            "\n";


    @Test
    public void example_config() throws InterruptedException {
        AtomicInteger taskConsumedCount = new AtomicInteger(0);
        DefaultDatabaseInitializer.createTable(PG_DEFAULT_TABLE_DDL, "example_task_table");

        SpringDatabaseAccessLayer databaseAccessLayer = new SpringDatabaseAccessLayer(
                DatabaseDialect.POSTGRESQL, QueueTableSchema.builder().build(),
                DefaultDatabaseInitializer.getJdbcTemplate(),
                DefaultDatabaseInitializer.getTransactionTemplate());
        QueueShard<SpringDatabaseAccessLayer> shard = new QueueShard<>(new QueueShardId("main"), databaseAccessLayer);

        QueueId queueId = new QueueId("example_queue");
        QueueSettings queueSettings = QueueSettings.builder()
                .withProcessingSettings(ProcessingSettings.builder()
                        .withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS)
                        .withThreadCount(1).build())
                .withPollSettings(PollSettings.builder()
                        .withBetweenTaskTimeout(Duration.ofMillis(100))
                        .withNoTaskTimeout(Duration.ofMillis(100))
                        .withFatalCrashTimeout(Duration.ofSeconds(1)).build())
                .withFailureSettings(FailureSettings.builder()
                        .withRetryType(FailRetryType.GEOMETRIC_BACKOFF)
                        .withRetryInterval(Duration.ofMinutes(1)).build())
                .withReenqueueSettings(ReenqueueSettings.builder()
                        .withRetryType(ReenqueueRetryType.MANUAL).build())
                .withExtSettings(ExtSettings.builder().withSettings(new LinkedHashMap<>()).build())
                .build();
        QueueConfig config = new QueueConfig(QueueLocation.builder().withTableName("example_task_table")
                .withQueueId(queueId).build(), queueSettings);


        ShardingQueueProducer<String, SpringDatabaseAccessLayer> shardingQueueProducer = new ShardingQueueProducer<>(
                config, NoopPayloadTransformer.getInstance(), new SingleQueueShardRouter<>(shard));
        QueueProducer<String> producer = new MonitoringQueueProducer<>(shardingQueueProducer, queueId);
        StringQueueConsumer consumer = new StringQueueConsumer(config, taskConsumedCount);

        QueueService queueService = new QueueService(singletonList(shard),
                new LoggingThreadLifecycleListener(),
                new LoggingTaskLifecycleListener());

        queueService.registerQueue(consumer);
        queueService.start();
        producer.enqueue(EnqueueParams.create("example task"));
        sleep(500);
        queueService.pause();
        producer.enqueue(EnqueueParams.create("example task"));
        sleep(500);
        queueService.unpause();
        sleep(500);
        queueService.shutdown();
        queueService.awaitTermination(Duration.ofSeconds(10));

        Assert.assertThat(taskConsumedCount.get(), equalTo(2));
    }

}
