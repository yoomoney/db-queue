package ru.yandex.money.common.dbqueue.init;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.Queue;
import ru.yandex.money.common.dbqueue.api.QueueExternalExecutor;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.QueueThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.api.ShardRouter;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.internal.QueueLoop;
import ru.yandex.money.common.dbqueue.internal.runner.QueueRunner;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class QueueExecutionPoolTest {

    @Test
    public void should_not_start_stop_when_not_initialized() throws Exception {
        QueueExecutionPool queueExecutionPool = new QueueExecutionPool(
                mock(QueueRegistry.class), mock(TaskLifecycleListener.class), mock(QueueThreadLifecycleListener.class));
        try {
            queueExecutionPool.shutdown();
            Assert.fail("should not shutdown");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("pool is not initialized"));
        }

        try {
            queueExecutionPool.start();
            Assert.fail("should not start");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("pool is not initialized"));
        }
    }

    @Test
    public void should_not_start_stop_when_invoked_twice() throws Exception {
        QueueRegistry queueRegistry = mock(QueueRegistry.class);
        when(queueRegistry.getQueues()).thenReturn(new ArrayList<>());
        when(queueRegistry.getExternalExecutors()).thenReturn(new HashMap<>());
        QueueExecutionPool queueExecutionPool = new QueueExecutionPool(
                queueRegistry, mock(TaskLifecycleListener.class), mock(QueueThreadLifecycleListener.class));
        queueExecutionPool.init();
        try {
            queueExecutionPool.start();
            queueExecutionPool.start();
            Assert.fail("should not start twice");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("queues already started"));
        }

        try {
            queueExecutionPool.shutdown();
            queueExecutionPool.shutdown();
            Assert.fail("should not shutdown twice");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("queues already stopped"));
        }
    }

    @Test
    public void should_start_and_stop_queue_on_two_shards_and_three_threads() throws Exception {
        QueueLocation location1 = QueueLocation.builder().withTableName("testTable")
                .withQueueName("testQueue1").build();
        QueueShardId shardId1 = new QueueShardId("s1");
        QueueShardId shardId2 = new QueueShardId("s2");
        QueueDao queueDao1 = mock(QueueDao.class);
        when(queueDao1.getShardId()).thenReturn(shardId1);
        QueueDao queueDao2 = mock(QueueDao.class);
        when(queueDao2.getShardId()).thenReturn(shardId2);

        QueueRegistry queueRegistry = mock(QueueRegistry.class);
        Queue queue = mock(Queue.class);
        when(queue.getQueueConfig()).thenReturn(new QueueConfig(
                location1,
                QueueSettings.builder()
                        .withNoTaskTimeout(Duration.ZERO)
                        .withThreadCount(3)
                        .withBetweenTaskTimeout(Duration.ZERO).build()));
        ShardRouter shardRouter = mock(ShardRouter.class);
        when(shardRouter.getShardsId()).thenReturn(new ArrayList() {{
            add(shardId1);
            add(shardId2);
        }});
        TaskLifecycleListener queueShardListener = mock(TaskLifecycleListener.class);
        QueueExternalExecutor externalExecutor = mock(QueueExternalExecutor.class);

        when(queue.getShardRouter()).thenReturn(shardRouter);
        when(queueRegistry.getQueues()).thenReturn(Collections.singletonList(queue));
        when(queueRegistry.getTaskListeners()).thenReturn(
                Collections.singletonMap(location1, queueShardListener));
        when(queueRegistry.getExternalExecutors()).thenReturn(
                Collections.singletonMap(location1, externalExecutor));
        when(queueRegistry.getShards()).thenReturn(new HashMap<QueueShardId, QueueDao>() {{
            put(shardId1, queueDao1);
            put(shardId2, queueDao2);
        }});

        ThreadFactory threadFactory = mock(ThreadFactory.class);
        TaskLifecycleListener defaultTaskListener = mock(TaskLifecycleListener.class);
        QueueThreadLifecycleListener threadListener = mock(QueueThreadLifecycleListener.class);
        ExecutorService queueThreadExecutor = spy(MoreExecutors.newDirectExecutorService());

        QueueLoop queueLoop = mock(QueueLoop.class);
        QueueRunner queueRunner = mock(QueueRunner.class);

        QueueExecutionPool queueExecutionPool = new QueueExecutionPool(queueRegistry,
                defaultTaskListener, threadListener,
                (location, shardId) -> threadFactory,
                (threadCount, factory) -> {
                    assertThat(factory, sameInstance(threadFactory));
                    assertThat(threadCount, equalTo(3));
                    return queueThreadExecutor;
                },
                listener -> {
                    assertThat(listener, sameInstance(threadListener));
                    return queueLoop;
                },
                poolInstance -> {
                    assertThat(poolInstance.queue, sameInstance(queue));
                    assertThat(poolInstance.externalExecutor, sameInstance(externalExecutor));
                    assertThat(poolInstance.taskListener, sameInstance(queueShardListener));
                    return queueRunner;
                });
        queueExecutionPool.init();
        queueExecutionPool.start();

        verify(queueThreadExecutor, times(2)).execute(any());
        verify(queueLoop, times(1)).start(shardId1, queue, queueRunner);
        verify(queueLoop, times(1)).start(shardId2, queue, queueRunner);

        queueExecutionPool.shutdown();

        verify(externalExecutor, times(1)).shutdownQueueExecutor();
        verify(queueThreadExecutor, times(2)).shutdownNow();
        verify(queueThreadExecutor, times(2)).awaitTermination(30L, TimeUnit.SECONDS);
    }
}