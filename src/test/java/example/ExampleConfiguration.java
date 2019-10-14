package example;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.config.DatabaseDialect;
import ru.yandex.money.common.dbqueue.config.QueueService;
import ru.yandex.money.common.dbqueue.config.QueueShard;
import ru.yandex.money.common.dbqueue.config.QueueShardId;
import ru.yandex.money.common.dbqueue.config.QueueTableSchema;
import ru.yandex.money.common.dbqueue.config.impl.NoopTaskLifecycleListener;
import ru.yandex.money.common.dbqueue.config.impl.NoopThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

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
        QueueDatabaseInitializer.createDefaultTable("example_task_table");

        QueueShard shard = new QueueShard(DatabaseDialect.POSTGRESQL,
                QueueTableSchema.builder().build(),
                new QueueShardId("main"),
                QueueDatabaseInitializer.getJdbcTemplate(),
                QueueDatabaseInitializer.getTransactionTemplate());

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
