package ru.yandex.money.common.dbqueue.spring;

import org.junit.Test;
import org.springframework.transaction.support.TransactionOperations;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.spring.impl.SpringTransactionalProducer;
import ru.yandex.money.common.dbqueue.stub.FakeTransactionTemplate;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
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

        QueueDao firstQueueDao = mock(QueueDao.class);
        when(firstQueueDao.getShardId()).thenReturn(firstShardId);
        when(firstQueueDao.getTransactionTemplate()).thenReturn(transactionTemplate);
        QueueDao secondQueueDao = mock(QueueDao.class);
        when(secondQueueDao.getShardId()).thenReturn(secondShardId);
        when(secondQueueDao.getTransactionTemplate()).thenReturn(transactionTemplate);
        when(secondQueueDao.enqueue(any(), any())).thenReturn(99L);

        SpringQueueProducer<String> producer = new SpringTransactionalProducer<>(queueId1, String.class);
        producer.setShards(new HashMap<QueueShardId, QueueDao>() {{
            put(firstShardId, firstQueueDao);
            put(secondShardId, secondQueueDao);
        }});
        TaskPayloadTransformer<String> payloadTransformer = mock(TaskPayloadTransformer.class);
        when(payloadTransformer.fromObject("testPayload")).thenReturn("transformedPayload");
        producer.setPayloadTransformer(payloadTransformer);
        SpringQueueShardRouter<String> shardRouter = spy(new SpringQueueShardRouter<String>(queueId1, String.class) {
            @Override
            public QueueShardId resolveShardId(EnqueueParams<String> enqueueParams) {
                return enqueueParams.getExecutionDelay().isZero() ? secondShardId : firstShardId;
            }

            @Override
            public Collection<QueueShardId> getShardsId() {
                return Arrays.asList(firstShardId, secondShardId);
            }
        });
        producer.setShardRouter(shardRouter);

        QueueConfig queueConfig = new QueueConfig(testLocation1, QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                .withNoTaskTimeout(Duration.ZERO).build());
        producer.setQueueConfig(queueConfig);


        EnqueueParams<String> enqueueParams = EnqueueParams.create("testPayload");
        Long taskId = producer.enqueue(enqueueParams);

        assertThat(taskId, equalTo(99L));

        verify(shardRouter).resolveShardId(enqueueParams);
        verify(payloadTransformer).fromObject(eq("testPayload"));
        verify(secondQueueDao).getTransactionTemplate();
        verify(transactionTemplate).execute(any());
        verify(secondQueueDao).enqueue(eq(testLocation1), eq(enqueueParams.withPayload("transformedPayload")));

    }

}