package ru.yandex.money.common.dbqueue.spring;

import org.junit.Test;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueShard;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.spring.impl.SpringTransactionalProducer;
import ru.yandex.money.common.dbqueue.stub.FakeTransactionTemplate;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 02.08.2017
 */
public class SpringTransactionalProducerTest {

    private static final QueueId queueId1 = new QueueId("test_queue1");
    private static final QueueLocation testLocation1 =
            QueueLocation.builder().withTableName("queue_test")
                    .withQueueId(queueId1).build();

    @Test
    public void should_enqueue_in_transaction() throws Exception {
        TransactionOperations transactionTemplate = spy(new FakeTransactionTemplate());
        QueueShardId firstShardId = new QueueShardId("s1");
        QueueShardId secondShardId = new QueueShardId("s2");

        QueueShard firstShard = new QueueShard(firstShardId, mock(JdbcOperations.class), transactionTemplate);
        QueueShard secondShard = spy(new QueueShard(secondShardId, mock(JdbcOperations.class), transactionTemplate));
        when(secondShard.getShardId()).thenReturn(secondShardId);
        when(secondShard.getTransactionTemplate()).thenReturn(transactionTemplate);
        QueueDao queueDao = mock(QueueDao.class);
        when(secondShard.getQueueDao()).thenReturn(queueDao);
        when(queueDao.enqueue(any(), any())).thenReturn(99L);

        SpringQueueProducer<String> producer = new SpringTransactionalProducer<>(queueId1, String.class);
        TaskPayloadTransformer<String> payloadTransformer = mock(TaskPayloadTransformer.class);
        when(payloadTransformer.fromObject("testPayload")).thenReturn("transformedPayload");
        producer.setPayloadTransformer(payloadTransformer);
        SpringQueueShardRouter<String> shardRouter = spy(new SpringQueueShardRouter<String>(queueId1, String.class) {
            @Nonnull
            @Override
            public QueueShard resolveEnqueuingShard(@Nonnull EnqueueParams<String> enqueueParams) {
                return enqueueParams.getExecutionDelay().isZero() ? secondShard : firstShard;
            }

            @Nonnull
            @Override
            public Collection<QueueShard> getProcessingShards() {
                return Arrays.asList(firstShard, secondShard);
            }

        });
        producer.setProducerShardRouter(shardRouter);

        QueueConfig queueConfig = new QueueConfig(testLocation1, QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                .withNoTaskTimeout(Duration.ZERO).build());
        producer.setQueueConfig(queueConfig);


        EnqueueParams<String> enqueueParams = EnqueueParams.create("testPayload");
        Long taskId = producer.enqueue(enqueueParams);

        assertThat(taskId, equalTo(99L));

        verify(shardRouter).resolveEnqueuingShard(enqueueParams);
        verify(payloadTransformer).fromObject(eq("testPayload"));
        verify(secondShard).getTransactionTemplate();
        verify(transactionTemplate).execute(any());
        verify(queueDao).enqueue(eq(testLocation1), eq(enqueueParams.withPayload("transformedPayload")));

    }

}