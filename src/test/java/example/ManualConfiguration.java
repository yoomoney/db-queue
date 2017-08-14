package example;

import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.PayloadTransformer;
import ru.yandex.money.common.dbqueue.api.Queue;
import ru.yandex.money.common.dbqueue.api.QueueAction;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.ShardRouter;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.api.impl.NoopPayloadTransformer;
import ru.yandex.money.common.dbqueue.api.impl.SingleShardRouter;
import ru.yandex.money.common.dbqueue.api.impl.TransactionalEnqueuer;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.init.QueueExecutionPool;
import ru.yandex.money.common.dbqueue.init.QueueRegistry;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
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
        QueueConfig config = new QueueConfig(new QueueLocation("example_manual_table", "example_queue"),
                QueueSettings.builder()
                        .withBetweenTaskTimeout(Duration.ofMillis(100))
                        .withNoTaskTimeout(Duration.ofSeconds(1))
                        .build());

        SingleShardRouter<String> shardRouter = new SingleShardRouter<>(queueDao);
        TransactionalEnqueuer<String> enqueuer = new TransactionalEnqueuer<>(config,
                NoopPayloadTransformer.getInstance(), Collections.singletonList(queueDao), shardRouter);
        ExampleQueue queue = new ExampleQueue(config, NoopPayloadTransformer.getInstance(), shardRouter);

        QueueRegistry queueRegistry = new QueueRegistry();
        queueRegistry.registerShard(queueDao);
        queueRegistry.registerQueue(queue, enqueuer);
        queueRegistry.finishRegistration();

        QueueExecutionPool executionPool = new QueueExecutionPool(queueRegistry, new EmptyTaskListener(),
                new EmptyQueueListener());
        executionPool.init();
        executionPool.start();

        executionPool.shutdown();
    }

    private static class ExampleQueue implements Queue<String> {

        private final QueueConfig queueConfig;
        private final PayloadTransformer<String> payloadTransformer;
        private final ShardRouter<String> shardRouter;

        private ExampleQueue(QueueConfig queueConfig, PayloadTransformer<String> payloadTransformer,
                             ShardRouter<String> shardRouter) {
            this.queueConfig = queueConfig;
            this.payloadTransformer = payloadTransformer;
            this.shardRouter = shardRouter;
        }


        @Nonnull
        @Override
        public QueueAction execute(@Nonnull Task<String> task) {
            return QueueAction.finish();
        }

        @Nonnull
        @Override
        public QueueConfig getQueueConfig() {
            return queueConfig;
        }

        @Nonnull
        @Override
        public PayloadTransformer<String> getPayloadTransformer() {
            return payloadTransformer;
        }

        @Nonnull
        @Override
        public ShardRouter<String> getShardRouter() {
            return shardRouter;
        }
    }


}
