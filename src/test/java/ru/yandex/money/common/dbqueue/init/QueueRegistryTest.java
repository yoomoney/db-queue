package ru.yandex.money.common.dbqueue.init;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import ru.yandex.money.common.dbqueue.api.Enqueuer;
import ru.yandex.money.common.dbqueue.api.PayloadTransformer;
import ru.yandex.money.common.dbqueue.api.Queue;
import ru.yandex.money.common.dbqueue.api.QueueAction;
import ru.yandex.money.common.dbqueue.api.QueueExternalExecutor;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.ShardRouter;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.stub.FakeEnqueuer;
import ru.yandex.money.common.dbqueue.stub.FakeQueue;

import java.time.Duration;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 02.08.2017
 */
public class QueueRegistryTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final QueueLocation testLocation1 =
            new QueueLocation("queue_test", "test_queue1");
    private static final QueueLocation testLocation2 =
            new QueueLocation("queue_test", "test_queue2");

    @Test
    public void should_fail_when_no_matching_queue_found() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "no matching queue for task listener: location={table=queue_test,queue=test_queue1}" + System.lineSeparator() +
                "no matching queue for external executor: location={table=queue_test,queue=test_queue1}"));

        QueueRegistry queueRegistry = new QueueRegistry();
        queueRegistry.registerExternalExecutor(testLocation1, mock(QueueExternalExecutor.class));
        queueRegistry.registerTaskLifecycleListener(testLocation1, mock(TaskLifecycleListener.class));
        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_when_enqueuer_does_not_match_queue() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "queue config must be the same: location={table=queue_test,queue=test_queue1}, enqueuer={location={table=queue_test,queue=test_queue1}, settings={threadCount=1, betweenTaskTimeout=PT0.1S, noTaskTimeout=PT0S, fatalCrashTimeout=PT2S, retryType=GEOMETRIC_BACKOFF, processingMode=SEPARATE_TRANSACTIONS}}, queue={location={table=queue_test,queue=test_queue1}, settings={threadCount=1, betweenTaskTimeout=PT0S, noTaskTimeout=PT0S, fatalCrashTimeout=PT2S, retryType=GEOMETRIC_BACKOFF, processingMode=SEPARATE_TRANSACTIONS}}" + System.lineSeparator() +
                "payload transformers must be the same: location={table=queue_test,queue=test_queue1}" + System.lineSeparator() +
                "shard routers must be the same: location={table=queue_test,queue=test_queue1}"));
        QueueRegistry queueRegistry = new QueueRegistry();
        queueRegistry.registerQueue(new FakeQueue(new QueueConfig(testLocation1,
                        QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO).build()),
                        mock(PayloadTransformer.class), mock(ShardRouter.class), t -> QueueAction.finish()),
                new FakeEnqueuer(new QueueConfig(testLocation1,
                        QueueSettings.builder().withBetweenTaskTimeout(Duration.ofMillis(100)).withNoTaskTimeout(Duration.ZERO).build()),
                        mock(PayloadTransformer.class), mock(ShardRouter.class), t -> 1L));
        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_when_duplicate_queue() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "duplicate queue: location={table=queue_test,queue=test_queue1}"));
        QueueRegistry queueRegistry = new QueueRegistry();
        QueueConfig queueConfig = new QueueConfig(testLocation1,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO).build());
        PayloadTransformer transformer = mock(PayloadTransformer.class);
        ShardRouter shardRouter = mock(ShardRouter.class);
        queueRegistry.registerQueue(new FakeQueue(queueConfig,
                        transformer, shardRouter, t -> QueueAction.finish()),
                new FakeEnqueuer(queueConfig,
                        transformer, shardRouter, t -> 1L));
        queueRegistry.registerQueue(new FakeQueue(queueConfig,
                        transformer, shardRouter, t -> QueueAction.finish()),
                new FakeEnqueuer(queueConfig,
                        transformer, shardRouter, t -> 1L));
        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_when_shards_is_not_used() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "shards is not used: shardIds=s2"));
        QueueRegistry queueRegistry = new QueueRegistry();
        PayloadTransformer transformer = mock(PayloadTransformer.class);

        ShardRouter shardRouter = mock(ShardRouter.class);
        when(shardRouter.getShardsId()).thenReturn(Collections.singletonList(new QueueShardId("s1")));
        QueueDao queueDao1 = mock(QueueDao.class);
        when(queueDao1.getShardId()).thenReturn(new QueueShardId("s1"));
        QueueConfig queueConfig1 = new QueueConfig(testLocation1,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO).build());
        queueRegistry.registerQueue(new FakeQueue(queueConfig1,
                        transformer, shardRouter, t -> QueueAction.finish()),
                new FakeEnqueuer(queueConfig1,
                        transformer, shardRouter, t -> 1L));

        QueueConfig queueConfig = new QueueConfig(testLocation2,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO).build());
        queueRegistry.registerQueue(new FakeQueue(queueConfig,
                        transformer, shardRouter, t -> QueueAction.finish()),
                new FakeEnqueuer(queueConfig,
                        transformer, shardRouter, t -> 1L));
        QueueDao queueDao2 = mock(QueueDao.class);
        when(queueDao2.getShardId()).thenReturn(new QueueShardId("s2"));

        queueRegistry.registerShard(queueDao1);
        queueRegistry.registerShard(queueDao2);

        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_when_shard_not_found() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "shard not found: shardId={id=s1}"));

        QueueRegistry queueRegistry = new QueueRegistry();
        QueueConfig queueConfig = new QueueConfig(testLocation1,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO).build());
        PayloadTransformer transformer = mock(PayloadTransformer.class);
        ShardRouter shardRouter = mock(ShardRouter.class);
        when(shardRouter.getShardsId()).thenReturn(Collections.singletonList(new QueueShardId("s1")));
        queueRegistry.registerQueue(new FakeQueue(queueConfig,
                        transformer, shardRouter, t -> QueueAction.finish()),
                new FakeEnqueuer(queueConfig,
                        transformer, shardRouter, t -> 1L));
        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_on_duplicate() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "duplicate external executor: location={table=queue_test,queue=test_queue1}" + System.lineSeparator() +
                "duplicate shard: shardId={id=s1}" + System.lineSeparator() +
                "duplicate task lifecycle listener: location={table=queue_test,queue=test_queue1}"));

        QueueRegistry queueRegistry = new QueueRegistry();

        QueueConfig queueConfig = new QueueConfig(testLocation1,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR)
                        .withNoTaskTimeout(Duration.ZERO).build());
        PayloadTransformer transformer = mock(PayloadTransformer.class);
        ShardRouter shardRouter = mock(ShardRouter.class);
        when(shardRouter.getShardsId()).thenReturn(Collections.singletonList(new QueueShardId("s1")));
        queueRegistry.registerQueue(new FakeQueue(queueConfig,
                        transformer, shardRouter, t -> QueueAction.finish()),
                new FakeEnqueuer(queueConfig,
                        transformer, shardRouter, t -> 1L));

        queueRegistry.registerExternalExecutor(testLocation1, mock(QueueExternalExecutor.class));
        queueRegistry.registerExternalExecutor(testLocation1, mock(QueueExternalExecutor.class));
        QueueDao queueDao = mock(QueueDao.class);
        when(queueDao.getShardId()).thenReturn(new QueueShardId("s1"));
        queueRegistry.registerShard(queueDao);
        queueRegistry.registerShard(queueDao);
        queueRegistry.registerTaskLifecycleListener(testLocation1, mock(TaskLifecycleListener.class));
        queueRegistry.registerTaskLifecycleListener(testLocation1, mock(TaskLifecycleListener.class));
        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_when_same_queue_belongs_to_different_tables() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "duplicate queue: location={table=table1,queue=queue}" + System.lineSeparator() +
                "queue name must be unique across all tables: queueName=queue, tables=[table1, table2]"));

        QueueRegistry queueRegistry = new QueueRegistry();

        QueueConfig queueConfig1 = new QueueConfig(new QueueLocation("table1", "queue"),
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build());
        QueueConfig queueConfig2 = new QueueConfig(new QueueLocation("table2", "queue"),
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build());
        QueueConfig queueConfig3 = new QueueConfig(new QueueLocation("table1", "queue"),
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build());

        PayloadTransformer transformer = mock(PayloadTransformer.class);
        ShardRouter shardRouter = mock(ShardRouter.class);
        when(shardRouter.getShardsId()).thenReturn(Collections.singletonList(new QueueShardId("s1")));
        queueRegistry.registerQueue(new FakeQueue(queueConfig1,
                        transformer, shardRouter, t -> QueueAction.finish()),
                new FakeEnqueuer(queueConfig1,
                        transformer, shardRouter, t -> 1L));
        queueRegistry.registerQueue(new FakeQueue(queueConfig2,
                        transformer, shardRouter, t -> QueueAction.finish()),
                new FakeEnqueuer(queueConfig2,
                        transformer, shardRouter, t -> 1L));
        queueRegistry.registerQueue(new FakeQueue(queueConfig3,
                        transformer, shardRouter, t -> QueueAction.finish()),
                new FakeEnqueuer(queueConfig3,
                        transformer, shardRouter, t -> 1L));

        QueueDao queueDao = mock(QueueDao.class);
        when(queueDao.getShardId()).thenReturn(new QueueShardId("s1"));
        queueRegistry.registerShard(queueDao);
        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_when_invalid_external_executor_config() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "external executor missing for processing mode USE_EXTERNAL_EXECUTOR: location={table=queue_test,queue=test_queue1}" + System.lineSeparator() +
                "external executor must be specified only for processing mode USE_EXTERNAL_EXECUTOR: location={table=queue_test,queue=test_queue2}"));

        QueueRegistry queueRegistry = new QueueRegistry();

        PayloadTransformer transformer = mock(PayloadTransformer.class);
        ShardRouter shardRouter = mock(ShardRouter.class);
        when(shardRouter.getShardsId()).thenReturn(Collections.singletonList(new QueueShardId("s1")));

        QueueConfig queueConfig1 = new QueueConfig(testLocation1,
                QueueSettings.builder()
                        .withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR)
                        .withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build());

        QueueConfig queueConfig2 = new QueueConfig(testLocation2,
                QueueSettings.builder()
                        .withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build());

        queueRegistry.registerQueue(new FakeQueue(queueConfig1,
                        transformer, shardRouter, t -> QueueAction.finish()),
                new FakeEnqueuer(queueConfig1,
                        transformer, shardRouter, t -> 1L));
        queueRegistry.registerQueue(new FakeQueue(queueConfig2,
                        transformer, shardRouter, t -> QueueAction.finish()),
                new FakeEnqueuer(queueConfig2,
                        transformer, shardRouter, t -> 1L));

        QueueDao queueDao = mock(QueueDao.class);
        when(queueDao.getShardId()).thenReturn(new QueueShardId("s1"));
        queueRegistry.registerShard(queueDao);

        queueRegistry.registerExternalExecutor(testLocation2, mock(QueueExternalExecutor.class));

        queueRegistry.finishRegistration();
    }

    @Test
    public void should_succesfully_register_all_settings() throws Exception {
        QueueRegistry queueRegistry = new QueueRegistry();

        QueueConfig queueConfig = new QueueConfig(testLocation1,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR)
                        .withNoTaskTimeout(Duration.ZERO).build());
        PayloadTransformer transformer = mock(PayloadTransformer.class);
        ShardRouter shardRouter = mock(ShardRouter.class);
        QueueShardId shardId = new QueueShardId("s1");
        when(shardRouter.getShardsId()).thenReturn(Collections.singletonList(shardId));
        FakeQueue queue = new FakeQueue(queueConfig,
                transformer, shardRouter, t -> QueueAction.finish());
        FakeEnqueuer enqueuer = new FakeEnqueuer(queueConfig,
                transformer, shardRouter, t -> 1L);
        queueRegistry.registerQueue(queue, enqueuer);

        QueueExternalExecutor externalExecutor = mock(QueueExternalExecutor.class);
        queueRegistry.registerExternalExecutor(testLocation1, externalExecutor);
        QueueDao queueDao = mock(QueueDao.class);
        when(queueDao.getShardId()).thenReturn(shardId);
        queueRegistry.registerShard(queueDao);
        TaskLifecycleListener taskLifecycleListener = mock(TaskLifecycleListener.class);
        queueRegistry.registerTaskLifecycleListener(testLocation1, taskLifecycleListener);
        queueRegistry.finishRegistration();

        Assert.assertThat(queueRegistry.getExternalExecutors(), equalTo(Collections.singletonMap(testLocation1, externalExecutor)));
        Assert.assertThat(queueRegistry.getShards(), equalTo(Collections.singletonMap(shardId, queueDao)));
        Assert.assertThat(queueRegistry.getTaskListeners(), equalTo(Collections.singletonMap(testLocation1, taskLifecycleListener)));
        Assert.assertThat(queueRegistry.getQueues(), hasItem(queue));
    }

    @Test
    public void should_not_register_when_construction_is_finished() throws Exception {
        QueueRegistry queueRegistry = new QueueRegistry();
        queueRegistry.finishRegistration();

        try {
            queueRegistry.registerQueue(mock(Queue.class), mock(Enqueuer.class));
            Assert.fail("should not do registration");
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), equalTo("cannot update property. construction is finished"));
        }
        try {
            queueRegistry.registerTaskLifecycleListener(testLocation1, mock(TaskLifecycleListener.class));
            Assert.fail("should not do registration");
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), equalTo("cannot update property. construction is finished"));
        }
        try {
            queueRegistry.registerShard(mock(QueueDao.class));
            Assert.fail("should not do registration");
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), equalTo("cannot update property. construction is finished"));
        }
        try {
            queueRegistry.registerExternalExecutor(testLocation1, mock(QueueExternalExecutor.class));
            Assert.fail("should not do registration");
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), equalTo("cannot update property. construction is finished"));
        }
    }

    @Test
    public void should_not_get_properties_when_construction_in_progress() throws Exception {
        QueueRegistry queueRegistry = new QueueRegistry();
        try {
            queueRegistry.getTaskListeners();
            Assert.fail("should not get properties");
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), equalTo("cannot get registry property. construction is not finished"));
        }
        try {
            queueRegistry.getShards();
            Assert.fail("should not get properties");
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), equalTo("cannot get registry property. construction is not finished"));
        }
        try {
            queueRegistry.getQueues();
            Assert.fail("should not get properties");
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), equalTo("cannot get registry property. construction is not finished"));
        }
        try {
            queueRegistry.getExternalExecutors();
            Assert.fail("should not get properties");
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), equalTo("cannot get registry property. construction is not finished"));
        }
    }


}