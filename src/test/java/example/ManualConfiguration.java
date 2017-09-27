package example;

import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.QueueShardRouter;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.api.impl.NoopPayloadTransformer;
import ru.yandex.money.common.dbqueue.api.impl.SingleShardRouter;
import ru.yandex.money.common.dbqueue.api.impl.TransactionalProducer;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.init.QueueExecutionPool;
import ru.yandex.money.common.dbqueue.init.QueueRegistry;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collections;

/**
 * @author Oleg Kandaurov
 * @since 14.08.2017
 */
public class ManualConfiguration {

    @Test
    public void manual_config() throws Exception {
        QueueDatabaseInitializer.createTable("example_manual_table");
        QueueDao queueDao = new QueueDao(new QueueShardId("master"), QueueDatabaseInitializer.getJdbcTemplate(),
                QueueDatabaseInitializer.getTransactionTemplate());
        QueueConfig config = new QueueConfig(QueueLocation.builder().withTableName("example_manual_table")
                .withQueueId(new QueueId("example_queue")).build(),
                QueueSettings.builder()
                        .withBetweenTaskTimeout(Duration.ofMillis(100))
                        .withNoTaskTimeout(Duration.ofSeconds(1))
                        .build());

        SingleShardRouter<String> shardRouter = new SingleShardRouter<>(queueDao);
        TransactionalProducer<String> producer = new TransactionalProducer<>(config,
                NoopPayloadTransformer.getInstance(), Collections.singletonList(queueDao), shardRouter);
        ExampleQueueConsumer consumer = new ExampleQueueConsumer(config, NoopPayloadTransformer.getInstance(), shardRouter);

        QueueRegistry queueRegistry = new QueueRegistry();
        queueRegistry.registerShard(queueDao);
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
        private final QueueShardRouter<String> shardRouter;

        private ExampleQueueConsumer(QueueConfig queueConfig, TaskPayloadTransformer<String> payloadTransformer,
                                     QueueShardRouter<String> shardRouter) {
            this.queueConfig = queueConfig;
            this.payloadTransformer = payloadTransformer;
            this.shardRouter = shardRouter;
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
        public QueueShardRouter<String> getShardRouter() {
            return shardRouter;
        }
    }


}
