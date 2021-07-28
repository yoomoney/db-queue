package ru.yoomoney.tech.dbqueue.api.impl;

import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.stub.StubDatabaseAccessLayer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class SingleQueueShardRouterTest {

    @Test
    public void should_return_single_shard() {
        QueueShard<StubDatabaseAccessLayer> main = new QueueShard<>(new QueueShardId("main"),
                new StubDatabaseAccessLayer());
        SingleQueueShardRouter<String, StubDatabaseAccessLayer> router = new SingleQueueShardRouter<>(main);
        assertThat(router.resolveShard(EnqueueParams.create("1")), equalTo(main));
    }
}