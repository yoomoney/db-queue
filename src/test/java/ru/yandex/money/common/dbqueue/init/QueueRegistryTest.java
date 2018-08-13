package ru.yandex.money.common.dbqueue.init;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueExternalExecutor;
import ru.yandex.money.common.dbqueue.api.QueueProducer;
import ru.yandex.money.common.dbqueue.api.QueueShard;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.QueueShardRouter;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.api.ThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.stub.FakeQueueConsumer;
import ru.yandex.money.common.dbqueue.stub.FakeQueueProducer;

import java.time.Duration;
import java.util.Arrays;
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

    private static final QueueId queueId1 = new QueueId("test_queue1");
    private static final QueueLocation testLocation1 =
            QueueLocation.builder().withTableName("queue_test")
                    .withQueueId(queueId1).build();

    private static final QueueId queueId2 = new QueueId("test_queue2");
    private static final QueueLocation testLocation2 =
            QueueLocation.builder().withTableName("queue_test")
                    .withQueueId(queueId2).build();

    @Test
    public void should_fail_when_no_matching_queue_found() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "no matching queue for task listener: queueId=test_queue1" + System.lineSeparator() +
                "no matching queue for thread listener: queueId=test_queue1" + System.lineSeparator() +
                "no matching queue for external executor: queueId=test_queue1"));

        QueueRegistry queueRegistry = new QueueRegistry();
        queueRegistry.registerExternalExecutor(queueId1, mock(QueueExternalExecutor.class));
        queueRegistry.registerTaskLifecycleListener(queueId1, mock(TaskLifecycleListener.class));
        queueRegistry.registerThreadLifecycleListener(queueId1, mock(ThreadLifecycleListener.class));
        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_when_producer_does_not_match_consumer() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "queue config must be the same: queueId=test_queue1, producer={location={id=test_queue1,table=queue_test}, settings={threadCount=1, betweenTaskTimeout=PT0.1S, noTaskTimeout=PT0S, processingMode=SEPARATE_TRANSACTIONS, retryType=GEOMETRIC_BACKOFF, retryInterval=PT1M, fatalCrashTimeout=PT1S}}, consumer={location={id=test_queue1,table=queue_test}, settings={threadCount=1, betweenTaskTimeout=PT0S, noTaskTimeout=PT0S, processingMode=SEPARATE_TRANSACTIONS, retryType=GEOMETRIC_BACKOFF, retryInterval=PT1M, fatalCrashTimeout=PT1S}}" + System.lineSeparator() +
                "payload transformers must be the same: queueId=test_queue1"));
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
                "duplicate queue: queueId=test_queue1"));
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
    public void should_fail_on_duplicate() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "duplicate shard: queueId=test_queue1, shardId=s1" + System.lineSeparator() +
                "duplicate external executor: queueId=test_queue1" + System.lineSeparator() +
                "duplicate task lifecycle listener: queueId=test_queue1" + System.lineSeparator() +
                "duplicate thread lifecycle listener: queueId=test_queue1"
        ));

        QueueRegistry queueRegistry = new QueueRegistry();

        QueueConfig queueConfig = new QueueConfig(testLocation1,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR)
                        .withNoTaskTimeout(Duration.ZERO).build());
        TaskPayloadTransformer transformer = mock(TaskPayloadTransformer.class);
        QueueShardRouter shardRouter = mock(QueueShardRouter.class);
        when(shardRouter.getProcessingShards()).thenReturn(Arrays.asList(
                new QueueShard(new QueueShardId("s1"), mock(JdbcOperations.class), mock(TransactionOperations.class)),
                new QueueShard(new QueueShardId("s1"), mock(JdbcOperations.class), mock(TransactionOperations.class))));
        queueRegistry.registerQueue(new FakeQueueConsumer(queueConfig,
                        transformer, shardRouter, t -> TaskExecutionResult.finish()),
                new FakeQueueProducer(queueConfig,
                        transformer, shardRouter, t -> 1L));

        queueRegistry.registerExternalExecutor(queueId1, mock(QueueExternalExecutor.class));
        queueRegistry.registerExternalExecutor(queueId1, mock(QueueExternalExecutor.class));
        queueRegistry.registerTaskLifecycleListener(queueId1, mock(TaskLifecycleListener.class));
        queueRegistry.registerTaskLifecycleListener(queueId1, mock(TaskLifecycleListener.class));
        queueRegistry.registerThreadLifecycleListener(queueId1, mock(ThreadLifecycleListener.class));
        queueRegistry.registerThreadLifecycleListener(queueId1, mock(ThreadLifecycleListener.class));
        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_when_same_queue_belongs_to_different_tables() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "duplicate queue: queueId=queue" + System.lineSeparator() +
                "duplicate queue: queueId=queue"));

        QueueRegistry queueRegistry = new QueueRegistry();

        QueueConfig queueConfig1 = new QueueConfig(QueueLocation.builder().withTableName("table1")
                .withQueueId(new QueueId("queue")).build(),
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build());
        QueueConfig queueConfig2 = new QueueConfig(QueueLocation.builder().withTableName("table2")
                .withQueueId(new QueueId("queue")).build(),
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build());
        QueueConfig queueConfig3 = new QueueConfig(QueueLocation.builder().withTableName("table1")
                .withQueueId(new QueueId("queue")).build(),
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build());

        TaskPayloadTransformer transformer = mock(TaskPayloadTransformer.class);
        QueueShardRouter shardRouter = mock(QueueShardRouter.class);
        when(shardRouter.getProcessingShards()).thenReturn(Collections.singletonList(
                new QueueShard(new QueueShardId("s1"), mock(JdbcOperations.class), mock(TransactionOperations.class))));
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

        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_when_invalid_external_executor_config() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Invalid queue configuration:" + System.lineSeparator() +
                "external executor missing for processing mode USE_EXTERNAL_EXECUTOR: queueId=test_queue1" + System.lineSeparator() +
                "external executor must be specified only for processing mode USE_EXTERNAL_EXECUTOR: queueId=test_queue2"));

        QueueRegistry queueRegistry = new QueueRegistry();

        TaskPayloadTransformer transformer = mock(TaskPayloadTransformer.class);
        QueueShardRouter shardRouter = mock(QueueShardRouter.class);
        when(shardRouter.getProcessingShards()).thenReturn(Collections.singletonList(
                new QueueShard(new QueueShardId("s1"), mock(JdbcOperations.class), mock(TransactionOperations.class))));

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

        queueRegistry.registerExternalExecutor(queueId2, mock(QueueExternalExecutor.class));

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
        FakeQueueConsumer consumer = new FakeQueueConsumer(queueConfig,
                transformer, shardRouter, t -> TaskExecutionResult.finish());
        FakeQueueProducer producer = new FakeQueueProducer(queueConfig,
                transformer, shardRouter, t -> 1L);
        queueRegistry.registerQueue(consumer, producer);

        QueueExternalExecutor externalExecutor = mock(QueueExternalExecutor.class);
        queueRegistry.registerExternalExecutor(queueId1, externalExecutor);

        TaskLifecycleListener taskLifecycleListener = mock(TaskLifecycleListener.class);
        queueRegistry.registerTaskLifecycleListener(queueId1, taskLifecycleListener);
        ThreadLifecycleListener threadLifecycleListener = mock(ThreadLifecycleListener.class);
        queueRegistry.registerThreadLifecycleListener(queueId1, threadLifecycleListener);
        queueRegistry.finishRegistration();

        Assert.assertThat(queueRegistry.getExternalExecutors(), equalTo(Collections.singletonMap(queueId1, externalExecutor)));
        Assert.assertThat(queueRegistry.getTaskListeners(), equalTo(Collections.singletonMap(queueId1, taskLifecycleListener)));
        Assert.assertThat(queueRegistry.getThreadListeners(), equalTo(Collections.singletonMap(queueId1, threadLifecycleListener)));
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
            queueRegistry.registerTaskLifecycleListener(queueId1, mock(TaskLifecycleListener.class));
            Assert.fail("should not do registration");
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), equalTo("cannot update property. construction is finished"));
        }
        try {
            queueRegistry.registerThreadLifecycleListener(queueId1, mock(ThreadLifecycleListener.class));
            Assert.fail("should not do registration");
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), equalTo("cannot update property. construction is finished"));
        }
        try {
            queueRegistry.registerExternalExecutor(queueId1, mock(QueueExternalExecutor.class));
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