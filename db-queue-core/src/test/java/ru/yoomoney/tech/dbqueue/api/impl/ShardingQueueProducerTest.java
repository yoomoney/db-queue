package ru.yoomoney.tech.dbqueue.api.impl;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.EnqueueResult;
import ru.yoomoney.tech.dbqueue.api.QueueShardRouter;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.dao.QueueDao;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.stub.StubDatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.stub.TestFixtures;

import java.util.Objects;

import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class ShardingQueueProducerTest {

    @Test
    public void should_insert_task_on_designated_shard() {

        StubDatabaseAccessLayer stubDatabaseAccessLayer = new StubDatabaseAccessLayer();
        QueueShard<StubDatabaseAccessLayer> firstShard = new QueueShard<>(new QueueShardId("first"),
                stubDatabaseAccessLayer);
        QueueShard<StubDatabaseAccessLayer> secondShard = new QueueShard<>(new QueueShardId("second"),
                stubDatabaseAccessLayer);

        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                        .withQueueId(new QueueId("main")).build(),
                TestFixtures.createQueueSettings().build());

        QueueDao queueDao = stubDatabaseAccessLayer.getQueueDao();
        when(queueDao.enqueue(eq(queueConfig.getLocation()), eq(EnqueueParams.create("1")))).thenReturn(11L);
        when(queueDao.enqueue(eq(queueConfig.getLocation()), eq(EnqueueParams.create("2")))).thenReturn(22L);

        ShardingQueueProducer<String, StubDatabaseAccessLayer> queueProducer = new ShardingQueueProducer<>(
                queueConfig, NoopPayloadTransformer.getInstance(), new StubQueueShardRouter(firstShard, secondShard));

        EnqueueResult enqueueResult1 = queueProducer.enqueue(EnqueueParams.create("1"));
        assertThat(enqueueResult1, CoreMatchers.equalTo(EnqueueResult.builder().withEnqueueId(11L)
                .withShardId(firstShard.getShardId()).build()));

        EnqueueResult enqueueResult2 = queueProducer.enqueue(EnqueueParams.create("2"));
        assertThat(enqueueResult2, CoreMatchers.equalTo(EnqueueResult.builder().withEnqueueId(22L)
                .withShardId(secondShard.getShardId()).build()));

    }

    private static class StubQueueShardRouter implements QueueShardRouter<String, StubDatabaseAccessLayer> {

        private final QueueShard<StubDatabaseAccessLayer> firstShard;
        private final QueueShard<StubDatabaseAccessLayer> secondShard;

        public StubQueueShardRouter(QueueShard<StubDatabaseAccessLayer> firstShard,
                                    QueueShard<StubDatabaseAccessLayer> secondShard) {
            this.firstShard = firstShard;
            this.secondShard = secondShard;
        }

        @Override
        public QueueShard<StubDatabaseAccessLayer> resolveShard(EnqueueParams<String> enqueueParams) {
            Objects.requireNonNull(enqueueParams.getPayload());
            if (enqueueParams.getPayload().equals("1")) {
                return firstShard;
            } else if (enqueueParams.getPayload().equals("2")) {
                return secondShard;
            }
            throw new IllegalStateException();
        }
    }
}