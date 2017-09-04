package ru.yandex.money.common.dbqueue.init;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueExternalExecutor;
import ru.yandex.money.common.dbqueue.api.QueueProducer;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.QueueShardRouter;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.api.ThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.stub.FakeQueueConsumer;
import ru.yandex.money.common.dbqueue.stub.FakeQueueProducer;

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
            QueueLocation.builder().withTableName("queue_test").withQueueName("test_queue1").build();
    private static final QueueLocation testLocation2 =
            QueueLocation.builder().withTableName("queue_test").withQueueName("test_queue2").build();

    @Test
    public void should_fail_when_no_matching_queue_found() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "no matching queue for task listener: location={queue=test_queue1,table=queue_test}" + System.lineSeparator() +
                "no matching queue for thread listener: location={queue=test_queue1,table=queue_test}" + System.lineSeparator() +
                "no matching queue for external executor: location={queue=test_queue1,table=queue_test}"));

        QueueRegistry queueRegistry = new QueueRegistry();
        queueRegistry.registerExternalExecutor(testLocation1, mock(QueueExternalExecutor.class));
        queueRegistry.registerTaskLifecycleListener(testLocation1, mock(TaskLifecycleListener.class));
        queueRegistry.registerThreadLifecycleListener(testLocation1, mock(ThreadLifecycleListener.class));
        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_when_producer_does_not_match_consumer() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "queue config must be the same: location={queue=test_queue1,table=queue_test}, producer={location={queue=test_queue1,table=queue_test}, settings={threadCount=1, betweenTaskTimeout=PT0.1S, noTaskTimeout=PT0S, processingMode=SEPARATE_TRANSACTIONS, retryType=GEOMETRIC_BACKOFF, retryInterval=PT1M, fatalCrashTimeout=PT1S}}, consumer={location={queue=test_queue1,table=queue_test}, settings={threadCount=1, betweenTaskTimeout=PT0S, noTaskTimeout=PT0S, processingMode=SEPARATE_TRANSACTIONS, retryType=GEOMETRIC_BACKOFF, retryInterval=PT1M, fatalCrashTimeout=PT1S}}" + System.lineSeparator() +
                "payload transformers must be the same: location={queue=test_queue1,table=queue_test}" + System.lineSeparator() +
                "shard routers must be the same: location={queue=test_queue1,table=queue_test}"));
        QueueRegistry queueRegistry = new QueueRegistry();
        queueRegistry.registerQueue(new FakeQueueConsumer(new QueueConfig(testLocation1,
                        QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO).build()),
                        mock(TaskPayloadTransformer.class), mock(QueueShardRouter.class), t -> TaskExecutionResult.finish()),
                new FakeQueueProducer(new QueueConfig(testLocation1,
                        QueueSettings.builder().withBetweenTaskTimeout(Duration.ofMillis(100)).withNoTaskTimeout(Duration.ZERO).build()),
                        mock(TaskPayloadTransformer.class), mock(QueueShardRouter.class), t -> 1L));
        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_when_duplicate_queue() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "duplicate queue: location={queue=test_queue1,table=queue_test}"));
        QueueRegistry queueRegistry = new QueueRegistry();
        QueueConfig queueConfig = new QueueConfig(testLocation1,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO).build());
        TaskPayloadTransformer transformer = mock(TaskPayloadTransformer.class);
        QueueShardRouter shardRouter = mock(QueueShardRouter.class);
        queueRegistry.registerQueue(new FakeQueueConsumer(queueConfig,
                        transformer, shardRouter, t -> TaskExecutionResult.finish()),
                new FakeQueueProducer(queueConfig,
                        transformer, shardRouter, t -> 1L));
        queueRegistry.registerQueue(new FakeQueueConsumer(queueConfig,
                        transformer, shardRouter, t -> TaskExecutionResult.finish()),
                new FakeQueueProducer(queueConfig,
                        transformer, shardRouter, t -> 1L));
        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_when_shards_is_not_used() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "shards is not used: shardIds=s2"));
        QueueRegistry queueRegistry = new QueueRegistry();
        TaskPayloadTransformer transformer = mock(TaskPayloadTransformer.class);

        QueueShardRouter shardRouter = mock(QueueShardRouter.class);
        when(shardRouter.getShardsId()).thenReturn(Collections.singletonList(new QueueShardId("s1")));
        QueueDao queueDao1 = mock(QueueDao.class);
        when(queueDao1.getShardId()).thenReturn(new QueueShardId("s1"));
        QueueConfig queueConfig1 = new QueueConfig(testLocation1,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO).build());
        queueRegistry.registerQueue(new FakeQueueConsumer(queueConfig1,
                        transformer, shardRouter, t -> TaskExecutionResult.finish()),
                new FakeQueueProducer(queueConfig1,
                        transformer, shardRouter, t -> 1L));

        QueueConfig queueConfig = new QueueConfig(testLocation2,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO).build());
        queueRegistry.registerQueue(new FakeQueueConsumer(queueConfig,
                        transformer, shardRouter, t -> TaskExecutionResult.finish()),
                new FakeQueueProducer(queueConfig,
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
        TaskPayloadTransformer transformer = mock(TaskPayloadTransformer.class);
        QueueShardRouter shardRouter = mock(QueueShardRouter.class);
        when(shardRouter.getShardsId()).thenReturn(Collections.singletonList(new QueueShardId("s1")));
        queueRegistry.registerQueue(new FakeQueueConsumer(queueConfig,
                        transformer, shardRouter, t -> TaskExecutionResult.finish()),
                new FakeQueueProducer(queueConfig,
                        transformer, shardRouter, t -> 1L));
        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_on_duplicate() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "duplicate external executor: location={queue=test_queue1,table=queue_test}" + System.lineSeparator() +
                "duplicate shard: shardId={id=s1}" + System.lineSeparator() +
                "duplicate task lifecycle listener: location={queue=test_queue1,table=queue_test}" + System.lineSeparator() +
                "duplicate thread lifecycle listener: location={queue=test_queue1,table=queue_test}"
        ));

        QueueRegistry queueRegistry = new QueueRegistry();

        QueueConfig queueConfig = new QueueConfig(testLocation1,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR)
                        .withNoTaskTimeout(Duration.ZERO).build());
        TaskPayloadTransformer transformer = mock(TaskPayloadTransformer.class);
        QueueShardRouter shardRouter = mock(QueueShardRouter.class);
        when(shardRouter.getShardsId()).thenReturn(Collections.singletonList(new QueueShardId("s1")));
        queueRegistry.registerQueue(new FakeQueueConsumer(queueConfig,
                        transformer, shardRouter, t -> TaskExecutionResult.finish()),
                new FakeQueueProducer(queueConfig,
                        transformer, shardRouter, t -> 1L));

        queueRegistry.registerExternalExecutor(testLocation1, mock(QueueExternalExecutor.class));
        queueRegistry.registerExternalExecutor(testLocation1, mock(QueueExternalExecutor.class));
        QueueDao queueDao = mock(QueueDao.class);
        when(queueDao.getShardId()).thenReturn(new QueueShardId("s1"));
        queueRegistry.registerShard(queueDao);
        queueRegistry.registerShard(queueDao);
        queueRegistry.registerTaskLifecycleListener(testLocation1, mock(TaskLifecycleListener.class));
        queueRegistry.registerTaskLifecycleListener(testLocation1, mock(TaskLifecycleListener.class));
        queueRegistry.registerThreadLifecycleListener(testLocation1, mock(ThreadLifecycleListener.class));
        queueRegistry.registerThreadLifecycleListener(testLocation1, mock(ThreadLifecycleListener.class));
        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_when_same_queue_belongs_to_different_tables() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "duplicate queue: location={queue=queue,table=table1}" + System.lineSeparator() +
                "queue name must be unique across all tables: queueName=queue, tables=[table1, table2]"));

        QueueRegistry queueRegistry = new QueueRegistry();

        QueueConfig queueConfig1 = new QueueConfig(QueueLocation.builder().withTableName("table1")
                .withQueueName("queue").build(),
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build());
        QueueConfig queueConfig2 = new QueueConfig(QueueLocation.builder().withTableName("table2")
                .withQueueName("queue").build(),
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build());
        QueueConfig queueConfig3 = new QueueConfig(QueueLocation.builder().withTableName("table1")
                .withQueueName("queue").build(),
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build());

        TaskPayloadTransformer transformer = mock(TaskPayloadTransformer.class);
        QueueShardRouter shardRouter = mock(QueueShardRouter.class);
        when(shardRouter.getShardsId()).thenReturn(Collections.singletonList(new QueueShardId("s1")));
        queueRegistry.registerQueue(new FakeQueueConsumer(queueConfig1,
                        transformer, shardRouter, t -> TaskExecutionResult.finish()),
                new FakeQueueProducer(queueConfig1,
                        transformer, shardRouter, t -> 1L));
        queueRegistry.registerQueue(new FakeQueueConsumer(queueConfig2,
                        transformer, shardRouter, t -> TaskExecutionResult.finish()),
                new FakeQueueProducer(queueConfig2,
                        transformer, shardRouter, t -> 1L));
        queueRegistry.registerQueue(new FakeQueueConsumer(queueConfig3,
                        transformer, shardRouter, t -> TaskExecutionResult.finish()),
                new FakeQueueProducer(queueConfig3,
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
                "external executor missing for processing mode USE_EXTERNAL_EXECUTOR: location={queue=test_queue1,table=queue_test}" + System.lineSeparator() +
                "external executor must be specified only for processing mode USE_EXTERNAL_EXECUTOR: location={queue=test_queue2,table=queue_test}"));

        QueueRegistry queueRegistry = new QueueRegistry();

        TaskPayloadTransformer transformer = mock(TaskPayloadTransformer.class);
        QueueShardRouter shardRouter = mock(QueueShardRouter.class);
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

        queueRegistry.registerQueue(new FakeQueueConsumer(queueConfig1,
                        transformer, shardRouter, t -> TaskExecutionResult.finish()),
                new FakeQueueProducer(queueConfig1,
                        transformer, shardRouter, t -> 1L));
        queueRegistry.registerQueue(new FakeQueueConsumer(queueConfig2,
                        transformer, shardRouter, t -> TaskExecutionResult.finish()),
                new FakeQueueProducer(queueConfig2,
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
        TaskPayloadTransformer transformer = mock(TaskPayloadTransformer.class);
        QueueShardRouter shardRouter = mock(QueueShardRouter.class);
        QueueShardId shardId = new QueueShardId("s1");
        when(shardRouter.getShardsId()).thenReturn(Collections.singletonList(shardId));
        FakeQueueConsumer consumer = new FakeQueueConsumer(queueConfig,
                transformer, shardRouter, t -> TaskExecutionResult.finish());
        FakeQueueProducer producer = new FakeQueueProducer(queueConfig,
                transformer, shardRouter, t -> 1L);
        queueRegistry.registerQueue(consumer, producer);

        QueueExternalExecutor externalExecutor = mock(QueueExternalExecutor.class);
        queueRegistry.registerExternalExecutor(testLocation1, externalExecutor);
        QueueDao queueDao = mock(QueueDao.class);
        when(queueDao.getShardId()).thenReturn(shardId);
        queueRegistry.registerShard(queueDao);
        TaskLifecycleListener taskLifecycleListener = mock(TaskLifecycleListener.class);
        queueRegistry.registerTaskLifecycleListener(testLocation1, taskLifecycleListener);
        ThreadLifecycleListener threadLifecycleListener = mock(ThreadLifecycleListener.class);
        queueRegistry.registerThreadLifecycleListener(testLocation1, threadLifecycleListener);
        queueRegistry.finishRegistration();

        Assert.assertThat(queueRegistry.getExternalExecutors(), equalTo(Collections.singletonMap(testLocation1, externalExecutor)));
        Assert.assertThat(queueRegistry.getShards(), equalTo(Collections.singletonMap(shardId, queueDao)));
        Assert.assertThat(queueRegistry.getTaskListeners(), equalTo(Collections.singletonMap(testLocation1, taskLifecycleListener)));
        Assert.assertThat(queueRegistry.getThreadListeners(), equalTo(Collections.singletonMap(testLocation1, threadLifecycleListener)));
        Assert.assertThat(queueRegistry.getConsumers(), hasItem(consumer));
    }

    @Test
    public void should_not_register_when_construction_is_finished() throws Exception {
        QueueRegistry queueRegistry = new QueueRegistry();
        queueRegistry.finishRegistration();

        try {
            queueRegistry.registerQueue(mock(QueueConsumer.class), mock(QueueProducer.class));
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
            queueRegistry.registerThreadLifecycleListener(testLocation1, mock(ThreadLifecycleListener.class));
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
            queueRegistry.getThreadListeners();
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
            queueRegistry.getConsumers();
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