package ru.yandex.money.common.dbqueue.example;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.money.common.dbqueue.api.Enqueuer;
import ru.yandex.money.common.dbqueue.api.PayloadTransformer;
import ru.yandex.money.common.dbqueue.api.Queue;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.ShardRouter;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.spring.SpringQueueConfigContainer;
import ru.yandex.money.common.dbqueue.spring.SpringSingleShardRouter;
import ru.yandex.money.common.dbqueue.spring.SpringTransactionalEnqueuer;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

import java.time.Duration;
import java.util.Collections;

/**
 * @author Oleg Kandaurov
 * @since 20.07.2017
 */
@Configuration
public class ExampleQueueConfiguration {
    private static final QueueLocation EXAMPLE_QUEUE_LOCATION =
            new QueueLocation("queue_test", "example_queue");

    @Bean
    SpringQueueConfigContainer springQueueConfigContainer() {
        return new SpringQueueConfigContainer(Collections.singletonList(new QueueConfig(
                EXAMPLE_QUEUE_LOCATION,
                QueueSettings.builder()
                        .withBetweenTaskTimeout(Duration.ofMillis(5L))
                        .withNoTaskTimeout(Duration.ofMillis(100L))
                        .build())));
    }

    @Bean
    QueueDao queueDao() {
        return new QueueDao(new QueueShardId("shard1"), QueueDatabaseInitializer.getJdbcTemplate(),
                QueueDatabaseInitializer.getTransactionTemplate());
    }

    @Bean
    Enqueuer<CustomPayload> exampleEnqueuer() {
        return new SpringTransactionalEnqueuer<>(EXAMPLE_QUEUE_LOCATION, CustomPayload.class);
    }

    @Bean
    Queue<CustomPayload> exampleQueue() {
        return new CustomQueue(EXAMPLE_QUEUE_LOCATION, CustomPayload.class);
    }

    @Bean
    PayloadTransformer<CustomPayload> exampleTransformer() {
        return new CustomPayloadTransformer(EXAMPLE_QUEUE_LOCATION, CustomPayload.class);
    }

    @Bean
    ShardRouter<CustomPayload> exampleShardRouter(QueueDao queueDao) {
        return new SpringSingleShardRouter<>(EXAMPLE_QUEUE_LOCATION, CustomPayload.class, queueDao);
    }
}
