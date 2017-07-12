package ru.yandex.money.common.dbqueue.init;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import ru.yandex.money.common.dbqueue.api.PayloadTransformer;
import ru.yandex.money.common.dbqueue.api.QueueAction;
import ru.yandex.money.common.dbqueue.api.QueueExternalExecutor;
import ru.yandex.money.common.dbqueue.api.ShardRouter;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.stub.FakeEnqueuer;
import ru.yandex.money.common.dbqueue.stub.FakeQueue;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;

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
                "queue config must be the same: location={table=queue_test,queue=test_queue1}, enqueuer={location={table=queue_test,queue=test_queue1}, settings={threadCount=1, betweenTaskTimeout=PT0S, noTaskTimeout=PT0S, fatalCrashTimeout=PT2S, retryType=GEOMETRIC_BACKOFF, processingMode=SEPARATE_TRANSACTIONS}}, queue={location={table=queue_test,queue=test_queue1}, settings={threadCount=1, betweenTaskTimeout=PT0S, noTaskTimeout=PT0S, fatalCrashTimeout=PT2S, retryType=GEOMETRIC_BACKOFF, processingMode=SEPARATE_TRANSACTIONS}}" + System.lineSeparator() +
                "payload transformers must be the same: location={table=queue_test,queue=test_queue1}" + System.lineSeparator() +
                "shard routers must be the same: location={table=queue_test,queue=test_queue1}"));
        QueueRegistry queueRegistry = new QueueRegistry();
        queueRegistry.registerQueue(new FakeQueue(new QueueConfig(testLocation1,
                        QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO).build()),
                mock(PayloadTransformer.class), mock(ShardRouter.class), t -> QueueAction.finish()),
                new FakeEnqueuer(new QueueConfig(testLocation1,
                        QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO).build()),
                mock(PayloadTransformer.class), mock(ShardRouter.class), t -> 1L));
        queueRegistry.finishRegistration();
    }

    @Test
    public void should_fail_when_duplicate_objects() throws Exception {
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

    }

    @Test
    public void should_fail_when_shard_not_found() throws Exception {

    }

    @Test
    public void should_fail_when_invalid_external_executor_config() throws Exception {

    }

    @Test
    public void should_succesfully_register_all_settings() throws Exception {

    }

    @Test
    public void should_not_register_when_construction_is_finished() throws Exception {

    }

    @Test
    public void should_get_properties_when_construction_in_progress() throws Exception {

    }
}