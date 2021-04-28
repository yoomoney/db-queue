package ru.yoomoney.tech.dbqueue.example;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.Task;
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.config.QueueService;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.config.impl.NoopTaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.config.impl.NoopThreadLifecycleListener;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.spring.dao.SpringDatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.spring.dao.utils.PostgresDatabaseInitializer;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;

/**
 * @author Oleg Kandaurov
 * @since 14.08.2017
 */
public class ExampleConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ExampleConfiguration.class);

    @Test
    public void example_config() throws InterruptedException {
        AtomicBoolean isTaskConsumed = new AtomicBoolean(false);
        PostgresDatabaseInitializer.initialize();
        PostgresDatabaseInitializer.createDefaultTable("example_task_table");
        SpringDatabaseAccessLayer databaseAccessLayer = new SpringDatabaseAccessLayer(
                DatabaseDialect.POSTGRESQL, QueueTableSchema.builder().build(),
                PostgresDatabaseInitializer.getJdbcTemplate(),
                PostgresDatabaseInitializer.getTransactionTemplate());
        QueueShard<SpringDatabaseAccessLayer> shard = new QueueShard<>(new QueueShardId("main"), databaseAccessLayer);

        QueueConfig config = new QueueConfig(QueueLocation.builder().withTableName("example_task_table")
                .withQueueId(new QueueId("example_queue")).build(),
                QueueSettings.builder()
                        .withBetweenTaskTimeout(Duration.ofMillis(100))
                        .withNoTaskTimeout(Duration.ofMillis(100))
                        .build());

        StringQueueProducer producer = new StringQueueProducer(config, shard);
        StringQueueConsumer consumer = new StringQueueConsumer(config) {
            @Nonnull
            @Override
            public TaskExecutionResult execute(@Nonnull Task<String> task) {
                log.info("payload={}", task.getPayloadOrThrow());
                isTaskConsumed.set(true);
                return TaskExecutionResult.finish();
            }
        };

        QueueService queueService = new QueueService(Collections.singletonList(shard),
                NoopThreadLifecycleListener.getInstance(), NoopTaskLifecycleListener.getInstance());
        queueService.registerQueue(consumer);
        queueService.start();
        producer.enqueue(EnqueueParams.create("example task"));
        sleep(1000);
        queueService.shutdown();
        queueService.awaitTermination(Duration.ofSeconds(10));
        Assert.assertTrue(isTaskConsumed.get());
    }

}
