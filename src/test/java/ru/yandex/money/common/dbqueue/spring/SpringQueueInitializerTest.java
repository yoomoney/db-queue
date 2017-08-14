package ru.yandex.money.common.dbqueue.spring;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueAction;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.init.QueueExecutionPool;
import ru.yandex.money.common.dbqueue.init.QueueRegistry;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 02.08.2017
 */
public class SpringQueueInitializerTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final QueueLocation testLocation1 =
            new QueueLocation("queue_test", "test_queue1");

    @Test
    public void should_not_throw_error_when_empty_configuration() throws Exception {
        QueueExecutionPool executionPool = mock(QueueExecutionPool.class);
        when(executionPool.getQueueRegistry()).thenReturn(new QueueRegistry());
        SpringQueueInitializer initializer = new SpringQueueInitializer(
                new SpringQueueConfigContainer(Collections.emptyList()),
                mock(SpringQueueCollector.class), executionPool);
        initializer.onApplicationEvent(null);
    }

    @Test
    public void should_start_and_stop_queues() throws Exception {
        QueueExecutionPool executionPool = mock(QueueExecutionPool.class);
        when(executionPool.getQueueRegistry()).thenReturn(new QueueRegistry());
        SpringQueueInitializer initializer = new SpringQueueInitializer(
                new SpringQueueConfigContainer(Collections.emptyList()),
                mock(SpringQueueCollector.class), executionPool);
        initializer.onApplicationEvent(null);
        verify(executionPool).init();
        verify(executionPool).start();
        initializer.destroy();
        verify(executionPool).shutdown();
    }

    @Test
    public void should_throw_when_missing_beans() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("unable to wire queue configuration:" + System.lineSeparator() +
                "queue config not found: location={table=queue_test,queue=test_queue1}" + System.lineSeparator() +
                "payload transformer not found: location={table=queue_test,queue=test_queue1}" + System.lineSeparator() +
                "shard router not found: location={table=queue_test,queue=test_queue1}" + System.lineSeparator() +
                "enqueuer not found: location={table=queue_test,queue=test_queue1}");
        QueueExecutionPool executionPool = mock(QueueExecutionPool.class);
        when(executionPool.getQueueRegistry()).thenReturn(new QueueRegistry());
        SpringQueueCollector queueCollector = mock(SpringQueueCollector.class);

        when(queueCollector.getQueues()).thenReturn(singletonMap(testLocation1, new SimpleQueue<>(String.class)));

        SpringQueueInitializer initializer = new SpringQueueInitializer(
                new SpringQueueConfigContainer(Collections.emptyList()),
                queueCollector, executionPool);
        initializer.onApplicationEvent(null);
    }

    @Test
    public void should_throw_when_transformers_not_match() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("unable to wire queue configuration:" + System.lineSeparator() +
                "payload transformer does not match queue: location={table=queue_test,queue=test_queue1}, queueClass=java.lang.Exception, transformerClass=java.lang.String" + System.lineSeparator() +
                "enqueuer does not match queue: location={table=queue_test,queue=test_queue1}, queueClass=java.lang.Exception, enqueuerClass=java.lang.Long" + System.lineSeparator() +
                "shard router does not match queue: location={table=queue_test,queue=test_queue1}, queueClass=java.lang.Exception, shardRouterClass=java.math.BigDecimal");

        QueueExecutionPool executionPool = mock(QueueExecutionPool.class);
        when(executionPool.getQueueRegistry()).thenReturn(new QueueRegistry());

        SpringQueueCollector queueCollector = mock(SpringQueueCollector.class);
        when(queueCollector.getQueues()).thenReturn(singletonMap(testLocation1, new SimpleQueue<>(Exception.class)));
        when(queueCollector.getEnqueuers()).thenReturn(singletonMap(testLocation1,
                new SpringTransactionalEnqueuer<>(testLocation1, Long.class)));
        when(queueCollector.getShardRouters()).thenReturn(singletonMap(testLocation1,
                new SpringSingleShardRouter<>(testLocation1, BigDecimal.class, mock(QueueDao.class))));
        when(queueCollector.getTransformers()).thenReturn(singletonMap(testLocation1,
                new SpringNoopPayloadTransformer(testLocation1)));

        SpringQueueInitializer initializer = new SpringQueueInitializer(
                new SpringQueueConfigContainer(Collections.singletonList(new QueueConfig(testLocation1,
                        QueueSettings.builder().withNoTaskTimeout(Duration.ofMillis(1L))
                                .withBetweenTaskTimeout(Duration.ofMillis(1L)).build()))),
                queueCollector, executionPool);
        initializer.onApplicationEvent(null);
    }

    @Test
    public void should_throw_when_unused_beans() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("unable to wire queue configuration:" + System.lineSeparator() +
                "unused enqueuer: location={table=queue_test,queue=test_queue1}" + System.lineSeparator() +
                "unused shard router: location={table=queue_test,queue=test_queue1}" + System.lineSeparator() +
                "unused transformer: location={table=queue_test,queue=test_queue1}" + System.lineSeparator() +
                "unused config: location={table=queue_test,queue=test_queue1}");
        QueueExecutionPool executionPool = mock(QueueExecutionPool.class);
        when(executionPool.getQueueRegistry()).thenReturn(new QueueRegistry());

        SpringQueueCollector queueCollector = mock(SpringQueueCollector.class);
        when(queueCollector.getEnqueuers()).thenReturn(singletonMap(testLocation1,
                new SpringTransactionalEnqueuer<>(testLocation1, String.class)));
        when(queueCollector.getShardRouters()).thenReturn(singletonMap(testLocation1,
                new SpringSingleShardRouter<>(testLocation1, String.class, mock(QueueDao.class))));
        when(queueCollector.getTransformers()).thenReturn(singletonMap(testLocation1,
                new SpringNoopPayloadTransformer(testLocation1)));

        SpringQueueInitializer initializer = new SpringQueueInitializer(
                new SpringQueueConfigContainer(Collections.singletonList(new QueueConfig(testLocation1,
                        QueueSettings.builder().withNoTaskTimeout(Duration.ofMillis(1L))
                                .withBetweenTaskTimeout(Duration.ofMillis(1L)).build()))),
                queueCollector, executionPool);
        initializer.onApplicationEvent(null);
    }

    @Test
    public void should_wire_queue() throws Exception {

        SpringQueueCollector queueCollector = mock(SpringQueueCollector.class);

        QueueConfig queueConfig = new QueueConfig(testLocation1,
                QueueSettings.builder().withNoTaskTimeout(Duration.ofMillis(1L))
                        .withBetweenTaskTimeout(Duration.ofMillis(1L)).build());

        QueueDao queueDao = mock(QueueDao.class);
        QueueShardId shardId = new QueueShardId("1");
        when(queueDao.getShardId()).thenReturn(shardId);

        QueueDao unusedQueueDao = mock(QueueDao.class);
        QueueShardId unusedShardId = new QueueShardId("unused");
        when(unusedQueueDao.getShardId()).thenReturn(unusedShardId);

        SimpleQueue<String> queue = new SimpleQueue<>(String.class);
        SpringEnqueuer<String> enqueuer = spy(new SpringTransactionalEnqueuer<>(testLocation1, String.class));
        SpringShardRouter<String> shardRouter = new SpringShardRouter<String>(testLocation1, String.class) {
            @Override
            public Collection<QueueShardId> getShardsId() {
                return Collections.singletonList(shardId);
            }

            @Override
            public QueueShardId resolveShardId(EnqueueParams enqueueParams) {
                return shardId;
            }
        };
        SpringNoopPayloadTransformer payloadTransformer = new SpringNoopPayloadTransformer(testLocation1);
        SpringQueueExternalExecutor externalExecutor = new SimpleExternalExecutor(testLocation1);
        SpringTaskLifecycleListener taskLifecycleListener = mock(SpringTaskLifecycleListener.class);
        when(taskLifecycleListener.getQueueLocation()).thenReturn(testLocation1);

        when(queueCollector.getQueues()).thenReturn(singletonMap(testLocation1, queue));
        when(queueCollector.getEnqueuers()).thenReturn(singletonMap(testLocation1, enqueuer));
        when(queueCollector.getShardRouters()).thenReturn(singletonMap(testLocation1, shardRouter));
        when(queueCollector.getTransformers()).thenReturn(singletonMap(testLocation1, payloadTransformer));
        when(queueCollector.getExecutors()).thenReturn(singletonMap(testLocation1, externalExecutor));
        when(queueCollector.getListeners()).thenReturn(singletonMap(testLocation1, taskLifecycleListener));
        when(queueCollector.getShards()).thenReturn(new HashMap<QueueShardId, QueueDao>() {{
            put(shardId, queueDao);
            put(unusedShardId, unusedQueueDao);
        }});

        QueueRegistry queueRegistry = mock(QueueRegistry.class);

        QueueExecutionPool executionPool = mock(QueueExecutionPool.class);
        when(executionPool.getQueueRegistry()).thenReturn(queueRegistry);
        SpringQueueInitializer initializer = new SpringQueueInitializer(
                new SpringQueueConfigContainer(Collections.singletonList(queueConfig)),
                queueCollector, executionPool);
        initializer.onApplicationEvent(null);

        verify(queueRegistry).registerShard(queueDao);
        verify(queueRegistry).registerQueue(queue, enqueuer);
        verify(queueRegistry).registerTaskLifecycleListener(testLocation1, taskLifecycleListener);
        verify(queueRegistry).registerExternalExecutor(testLocation1, externalExecutor);
        verify(queueRegistry).finishRegistration();

        assertThat(queue.getShardRouter(), equalTo(shardRouter));
        assertThat(queue.getPayloadTransformer(), equalTo(payloadTransformer));
        assertThat(queue.getQueueConfig(), equalTo(queueConfig));

        assertThat(enqueuer.getShardRouter(), equalTo(shardRouter));
        assertThat(enqueuer.getPayloadTransformer(), equalTo(payloadTransformer));
        assertThat(enqueuer.getQueueConfig(), equalTo(queueConfig));
        HashMap<QueueShardId, QueueDao> expectedShards = new HashMap<QueueShardId, QueueDao>() {{
            put(shardId, queueDao);
        }};
        verify(enqueuer).setShards(eq(expectedShards));
        assertThat(enqueuer.getQueueConfig(), equalTo(queueConfig));

    }

    private static class SimpleQueue<T> extends SpringQueue<T> {
        public SimpleQueue(Class<T> clazz) {
            super(testLocation1, clazz);
        }

        @Nonnull
        @Override
        public QueueAction execute(@Nonnull Task<T> task) {
            return QueueAction.finish();
        }
    }

    private static class SimpleExternalExecutor implements SpringQueueExternalExecutor {
        private final QueueLocation location;

        public SimpleExternalExecutor(QueueLocation location) {
            this.location = location;
        }

        @Override
        public void shutdownQueueExecutor() {

        }

        @Override
        public void execute(Runnable command) {

        }

        @Nonnull
        @Override
        public QueueLocation getQueueLocation() {
            return location;
        }
    }
}