package example;

import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueShard;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.QueueShardRouter;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.api.impl.NoopPayloadTransformer;
import ru.yandex.money.common.dbqueue.api.impl.TransactionalProducer;
import ru.yandex.money.common.dbqueue.init.QueueExecutionPool;
import ru.yandex.money.common.dbqueue.init.QueueRegistry;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

/**
 * @author Oleg Kandaurov
 * @since 14.08.2017
 */
public class ManualConfiguration {

    @Test
    public void manual_config() {
        QueueDatabaseInitializer.createTable("example_manual_table");
        QueueShard queueShard = new QueueShard(new QueueShardId("master"), QueueDatabaseInitializer.getJdbcTemplate(),
                QueueDatabaseInitializer.getTransactionTemplate());
        QueueConfig config = new QueueConfig(QueueLocation.builder().withTableName("example_manual_table")
                .withQueueId(new QueueId("example_queue")).build(),
                QueueSettings.builder()
                        .withBetweenTaskTimeout(Duration.ofMillis(100))
                        .withNoTaskTimeout(Duration.ofSeconds(1))
                        .build());

        QueueShardRouter<String> shardRouter = new QueueShardRouter<String>() {
            @Nonnull
            @Override
            public QueueShard resolveEnqueuingShard(@Nonnull EnqueueParams<String> enqueueParams) {
                return queueShard;
            }

            @Nonnull
            @Override
            public Collection<QueueShard> getProcessingShards() {
                return Collections.singletonList(queueShard);
            }
        };
        TransactionalProducer<String> producer = new TransactionalProducer<>(config,
                NoopPayloadTransformer.getInstance(), shardRouter);
        ExampleQueueConsumer consumer = new ExampleQueueConsumer(config, NoopPayloadTransformer.getInstance(), shardRouter);

        QueueRegistry queueRegistry = new QueueRegistry();
        queueRegistry.registerQueue(consumer, producer);
        queueRegistry.finishRegistration();

        QueueExecutionPool executionPool = new QueueExecutionPool(queueRegistry, new EmptyTaskListener(),
                new EmptyListener());
        executionPool.init();
        executionPool.start();

        executionPool.shutdown();
    }

    private static class ExampleQueueConsumer implements QueueConsumer<String> {

        private final QueueConfig queueConfig;
        private final TaskPayloadTransformer<String> payloadTransformer;
        private final QueueConsumer.ConsumerShardsProvider consumerShardsProvider;

        private ExampleQueueConsumer(QueueConfig queueConfig, TaskPayloadTransformer<String> payloadTransformer,
                                     QueueConsumer.ConsumerShardsProvider consumerShardsProvider) {
            this.queueConfig = queueConfig;
            this.payloadTransformer = payloadTransformer;
            this.consumerShardsProvider = consumerShardsProvider;
        }


        @Nonnull
        @Override
        public TaskExecutionResult execute(@Nonnull Task<String> task) {
            return TaskExecutionResult.finish();
        }

        @Nonnull
        @Override
        public QueueConfig getQueueConfig() {
            return queueConfig;
        }

        @Nonnull
        @Override
        public TaskPayloadTransformer<String> getPayloadTransformer() {
            return payloadTransformer;
        }

        @Nonnull
        @Override
        public ConsumerShardsProvider getConsumerShardsProvider() {
            return consumerShardsProvider;
        }
    }


}
