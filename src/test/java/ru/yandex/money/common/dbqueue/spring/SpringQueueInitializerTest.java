package ru.yandex.money.common.dbqueue.spring;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.init.QueueExecutionPool;
import ru.yandex.money.common.dbqueue.init.QueueRegistry;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.spring.impl.SpringNoopPayloadTransformer;
import ru.yandex.money.common.dbqueue.spring.impl.SpringSingleShardRouter;
import ru.yandex.money.common.dbqueue.spring.impl.SpringTransactionalProducer;

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
            QueueLocation.builder().withTableName("queue_test").withQueueName("test_queue1").build();

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
                "consumer config not found: location={queue=test_queue1,table=queue_test}" + System.lineSeparator() +
                "payload transformer not found: location={queue=test_queue1,table=queue_test}" + System.lineSeparator() +
                "shard router not found: location={queue=test_queue1,table=queue_test}" + System.lineSeparator() +
                "producer not found: location={queue=test_queue1,table=queue_test}");
        QueueExecutionPool executionPool = mock(QueueExecutionPool.class);
        when(executionPool.getQueueRegistry()).thenReturn(new QueueRegistry());
        SpringQueueCollector queueCollector = mock(SpringQueueCollector.class);

        when(queueCollector.getConsumers()).thenReturn(singletonMap(testLocation1, new SimpleQueueConsumer<>(String.class)));

        SpringQueueInitializer initializer = new SpringQueueInitializer(
                new SpringQueueConfigContainer(Collections.emptyList()),
                queueCollector, executionPool);
        initializer.onApplicationEvent(null);
    }

    @Test
    public void should_throw_when_transformers_not_match() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("unable to wire queue configuration:" + System.lineSeparator() +
                "payload transformer does not match consumer: location={queue=test_queue1,table=queue_test}, consumerClass=java.lang.Exception, transformerClass=java.lang.String" + System.lineSeparator() +
                "producer does not match consumer: location={queue=test_queue1,table=queue_test}, consumerClass=java.lang.Exception, producerClass=java.lang.Long" + System.lineSeparator() +
                "shard router does not match consumer: location={queue=test_queue1,table=queue_test}, consumerClass=java.lang.Exception, shardRouterClass=java.math.BigDecimal");

        QueueExecutionPool executionPool = mock(QueueExecutionPool.class);
        when(executionPool.getQueueRegistry()).thenReturn(new QueueRegistry());

        SpringQueueCollector queueCollector = mock(SpringQueueCollector.class);
        when(queueCollector.getConsumers()).thenReturn(singletonMap(testLocation1, new SimpleQueueConsumer<>(Exception.class)));
        when(queueCollector.getProducers()).thenReturn(singletonMap(testLocation1,
                new SpringTransactionalProducer<>(testLocation1, Long.class)));
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
                "unused producer: location={queue=test_queue1,table=queue_test}" + System.lineSeparator() +
                "unused shard router: location={queue=test_queue1,table=queue_test}" + System.lineSeparator() +
                "unused transformer: location={queue=test_queue1,table=queue_test}" + System.lineSeparator() +
                "unused config: location={queue=test_queue1,table=queue_test}");
        QueueExecutionPool executionPool = mock(QueueExecutionPool.class);
        when(executionPool.getQueueRegistry()).thenReturn(new QueueRegistry());

        SpringQueueCollector queueCollector = mock(SpringQueueCollector.class);
        when(queueCollector.getProducers()).thenReturn(singletonMap(testLocation1,
                new SpringTransactionalProducer<>(testLocation1, String.class)));
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

        SimpleQueueConsumer<String> consumer = new SimpleQueueConsumer<>(String.class);
        SpringQueueProducer<String> producer = spy(new SpringTransactionalProducer<>(testLocation1, String.class));
        SpringQueueShardRouter<String> shardRouter = new SpringQueueShardRouter<String>(testLocation1, String.class) {
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

        when(queueCollector.getConsumers()).thenReturn(singletonMap(testLocation1, consumer));
        when(queueCollector.getProducers()).thenReturn(singletonMap(testLocation1, producer));
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
        verify(queueRegistry).registerQueue(consumer, producer);
        verify(queueRegistry).registerTaskLifecycleListener(testLocation1, taskLifecycleListener);
        verify(queueRegistry).registerExternalExecutor(testLocation1, externalExecutor);
        verify(queueRegistry).finishRegistration();

        assertThat(consumer.getShardRouter(), equalTo(shardRouter));
        assertThat(consumer.getPayloadTransformer(), equalTo(payloadTransformer));
        assertThat(consumer.getQueueConfig(), equalTo(queueConfig));

        assertThat(producer.getShardRouter(), equalTo(shardRouter));
        assertThat(producer.getPayloadTransformer(), equalTo(payloadTransformer));
        assertThat(producer.getQueueConfig(), equalTo(queueConfig));
        HashMap<QueueShardId, QueueDao> expectedShards = new HashMap<QueueShardId, QueueDao>() {{
            put(shardId, queueDao);
        }};
        verify(producer).setShards(eq(expectedShards));
        assertThat(producer.getQueueConfig(), equalTo(queueConfig));

    }

    private static class SimpleQueueConsumer<T> extends SpringQueueConsumer<T> {
        public SimpleQueueConsumer(Class<T> clazz) {
            super(testLocation1, clazz);
        }

        @Nonnull
        @Override
        public TaskExecutionResult execute(@Nonnull Task<T> task) {
            return TaskExecutionResult.finish();
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