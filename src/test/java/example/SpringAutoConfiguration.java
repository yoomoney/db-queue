package example;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueProducer;
import ru.yandex.money.common.dbqueue.api.QueueShard;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.QueueShardRouter;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.init.QueueExecutionPool;
import ru.yandex.money.common.dbqueue.init.QueueRegistry;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.spring.SpringQueueCollector;
import ru.yandex.money.common.dbqueue.spring.SpringQueueConfigContainer;
import ru.yandex.money.common.dbqueue.spring.SpringQueueConsumer;
import ru.yandex.money.common.dbqueue.spring.SpringQueueInitializer;
import ru.yandex.money.common.dbqueue.spring.SpringQueueShardRouter;
import ru.yandex.money.common.dbqueue.spring.impl.SpringNoopPayloadTransformer;
import ru.yandex.money.common.dbqueue.spring.impl.SpringTransactionalProducer;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

/**
 * @author Oleg Kandaurov
 * @since 14.08.2017
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SpringAutoConfiguration.Base.class, SpringAutoConfiguration.Client.class})
public class SpringAutoConfiguration {

    private static final QueueId EXAMPLE_QUEUE = new QueueId("example_queue");

    @Test
    public void spring_auto_config() throws Exception {
        Assert.assertTrue(true);
    }

    @ContextConfiguration
    public static class Client {
        @Bean
        QueueConsumer<String> exampleQueue() {
            return new SpringQueueConsumer<String>(EXAMPLE_QUEUE, String.class) {
                @Nonnull
                @Override
                public TaskExecutionResult execute(@Nonnull Task<String> task) {
                    return TaskExecutionResult.finish();
                }
            };
        }

        @Bean
        QueueProducer<String> exampleProducer() {
            return new SpringTransactionalProducer<>(EXAMPLE_QUEUE, String.class);
        }

        @Bean
        QueueShardRouter<String> exampleShardRouter() {
            return new SpringQueueShardRouter<String>(EXAMPLE_QUEUE, String.class) {

                private final QueueShard singleShard = new QueueShard(new QueueShardId("master"),
                        QueueDatabaseInitializer.getJdbcTemplate(),
                        QueueDatabaseInitializer.getTransactionTemplate());

                @Nonnull
                @Override
                public Collection<QueueShard> getProcessingShards() {
                    return Collections.singletonList(singleShard);
                }

                @Nonnull
                @Override
                public QueueShard resolveEnqueuingShard(@Nonnull EnqueueParams<String> enqueueParams) {
                    return singleShard;
                }
            };
        }

        @Bean
        TaskPayloadTransformer<String> examplePayloadTransformer() {
            return new SpringNoopPayloadTransformer(EXAMPLE_QUEUE);
        }
    }

    @ContextConfiguration
    public static class Base {

        @Bean
        SpringQueueConfigContainer springQueueConfigContainer() {
            return new SpringQueueConfigContainer(Collections.singletonList(new QueueConfig(
                    QueueLocation.builder().withTableName("example_spring_table")
                            .withQueueId(EXAMPLE_QUEUE).build(),
                    QueueSettings.builder()
                            .withBetweenTaskTimeout(Duration.ofMillis(100L))
                            .withNoTaskTimeout(Duration.ofSeconds(1L))
                            .build())));
        }

        @Bean
        SpringQueueCollector springQueueCollector() {
            return new SpringQueueCollector();
        }

        @Bean
        SpringQueueInitializer springQueueInitializer(SpringQueueConfigContainer springQueueConfigContainer,
                                                      SpringQueueCollector springQueueCollector) {
            return new SpringQueueInitializer(springQueueConfigContainer, springQueueCollector,
                    new QueueExecutionPool(new QueueRegistry(), new EmptyTaskListener(), new EmptyListener()));
        }
    }

}
