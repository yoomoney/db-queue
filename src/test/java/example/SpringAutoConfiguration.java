package example;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import ru.yandex.money.common.dbqueue.api.Enqueuer;
import ru.yandex.money.common.dbqueue.api.PayloadTransformer;
import ru.yandex.money.common.dbqueue.api.Queue;
import ru.yandex.money.common.dbqueue.api.QueueAction;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.ShardRouter;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.init.QueueExecutionPool;
import ru.yandex.money.common.dbqueue.init.QueueRegistry;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.spring.SpringQueue;
import ru.yandex.money.common.dbqueue.spring.SpringQueueCollector;
import ru.yandex.money.common.dbqueue.spring.SpringQueueConfigContainer;
import ru.yandex.money.common.dbqueue.spring.SpringQueueInitializer;
import ru.yandex.money.common.dbqueue.spring.impl.SpringNoopPayloadTransformer;
import ru.yandex.money.common.dbqueue.spring.impl.SpringSingleShardRouter;
import ru.yandex.money.common.dbqueue.spring.impl.SpringTransactionalEnqueuer;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collections;

/**
 * @author Oleg Kandaurov
 * @since 14.08.2017
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SpringAutoConfiguration.Base.class, SpringAutoConfiguration.Client.class})
public class SpringAutoConfiguration {

    private static final QueueLocation EXAMPLE_QUEUE =
            QueueLocation.builder().withTableName("example_spring_table").withQueueName("example_queue").build();

    @Test
    public void spring_auto_config() throws Exception {
        Assert.assertTrue(true);
    }

    @ContextConfiguration
    public static class Client {
        @Bean
        Queue<String> exampleQueue() {
            return new SpringQueue<String>(EXAMPLE_QUEUE, String.class) {
                @Nonnull
                @Override
                public QueueAction execute(@Nonnull Task<String> task) {
                    return QueueAction.finish();
                }
            };
        }

        @Bean
        Enqueuer<String> exampleEnqueuer() {
            return new SpringTransactionalEnqueuer<>(EXAMPLE_QUEUE, String.class);
        }

        @Bean
        ShardRouter<String> exampleShardRouter(QueueDao queueDao) {
            return new SpringSingleShardRouter<>(EXAMPLE_QUEUE, String.class, queueDao);
        }

        @Bean
        PayloadTransformer<String> examplePayloadTransformer() {
            return new SpringNoopPayloadTransformer(EXAMPLE_QUEUE);
        }
    }

    @ContextConfiguration
    public static class Base {

        @Bean
        SpringQueueConfigContainer springQueueConfigContainer() {
            return new SpringQueueConfigContainer(Collections.singletonList(new QueueConfig(
                    EXAMPLE_QUEUE,
                    QueueSettings.builder()
                            .withBetweenTaskTimeout(Duration.ofMillis(100L))
                            .withNoTaskTimeout(Duration.ofSeconds(1L))
                            .build())));
        }

        @Bean
        QueueDao queueDao() {
            QueueDatabaseInitializer.createTable("example_spring_table");
            return new QueueDao(new QueueShardId("master"), QueueDatabaseInitializer.getJdbcTemplate(),
                    QueueDatabaseInitializer.getTransactionTemplate());
        }

        @Bean
        SpringQueueCollector springQueueCollector() {
            return new SpringQueueCollector();
        }

        @Bean
        SpringQueueInitializer springQueueInitializer(SpringQueueConfigContainer springQueueConfigContainer,
                                                      SpringQueueCollector springQueueCollector) {
            return new SpringQueueInitializer(springQueueConfigContainer, springQueueCollector,
                    new QueueExecutionPool(new QueueRegistry(), new EmptyTaskListener(),
                            new EmptyQueueListener()));
        }
    }

}
