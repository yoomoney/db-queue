package ru.yoomoney.tech.dbqueue.config;

import org.junit.Test;
import ru.yoomoney.tech.dbqueue.internal.processing.QueueLoop;
import ru.yoomoney.tech.dbqueue.internal.processing.QueueTaskPoller;
import ru.yoomoney.tech.dbqueue.internal.processing.SyncQueueLoop;
import ru.yoomoney.tech.dbqueue.internal.runner.QueueRunner;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.stub.NoopQueueConsumer;
import ru.yoomoney.tech.dbqueue.stub.StringQueueConsumer;
import ru.yoomoney.tech.dbqueue.stub.StubDatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.stub.TestFixtures;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class QueueExecutionPoolTest {

    private static final QueueShard<?> DEFAULT_SHARD = new QueueShard<>(new QueueShardId("s1"),
            new StubDatabaseAccessLayer());

    @Test
    public void should_start() {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                        .withQueueId(new QueueId("queue1")).build(),
                TestFixtures.createQueueSettings().withProcessingSettings(
                                TestFixtures.createProcessingSettings().withThreadCount(2).build())
                        .build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueTaskPoller queueTaskPoller = mock(QueueTaskPoller.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        ExecutorService executor = spy(new DirectExecutor());
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueTaskPoller, executor,
                queueRunner, () -> queueLoop);
        pool.start();
        verify(queueTaskPoller, times(2)).start(queueLoop, DEFAULT_SHARD.getShardId(), consumer, queueRunner);
        verify(executor, times(2)).submit(any(Runnable.class));
    }

    @Test
    public void should_shutdown() {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                TestFixtures.createQueueSettings().build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueTaskPoller queueTaskPoller = mock(QueueTaskPoller.class);
        ExecutorService executor = mock(ExecutorService.class);
        when(executor.submit(any(Runnable.class))).thenReturn(mock(Future.class));
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner,
                SyncQueueLoop::new);
        pool.start();
        pool.shutdown();
        verify(executor).shutdownNow();
    }

    @Test
    public void should_pause() {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                TestFixtures.createQueueSettings().build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueTaskPoller queueTaskPoller = mock(QueueTaskPoller.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        ExecutorService executor = mock(ExecutorService.class);
        when(executor.submit(any(Runnable.class))).thenReturn(mock(Future.class));
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner,
                () -> queueLoop);
        pool.start();
        pool.pause();
        verify(queueLoop).pause();
    }

    @Test
    public void should_invoke_ispaused() {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                TestFixtures.createQueueSettings().build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueTaskPoller queueTaskPoller = mock(QueueTaskPoller.class);
        ExecutorService executor = mock(ExecutorService.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        when(executor.submit(any(Runnable.class))).thenReturn(mock(Future.class));
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner,
                () -> queueLoop);
        pool.start();
        pool.isPaused();
        verify(queueLoop).isPaused();
    }

    @Test
    public void should_invoke_isterminated() {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                TestFixtures.createQueueSettings().build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueTaskPoller queueTaskPoller = mock(QueueTaskPoller.class);
        ExecutorService executor = mock(ExecutorService.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner,
                () -> queueLoop);
        pool.isTerminated();
        verify(executor).isTerminated();
    }

    @Test
    public void should_invoke_isshutdown() {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                TestFixtures.createQueueSettings().build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueTaskPoller queueTaskPoller = mock(QueueTaskPoller.class);
        ExecutorService executor = mock(ExecutorService.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner,
                () -> queueLoop);
        pool.isShutdown();
        verify(executor).isShutdown();
    }

    @Test
    public void should_await_termination() throws InterruptedException {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                TestFixtures.createQueueSettings().build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueTaskPoller queueTaskPoller = mock(QueueTaskPoller.class);
        ExecutorService executor = mock(ExecutorService.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner,
                () -> queueLoop);
        pool.awaitTermination(Duration.ofSeconds(10));
        verify(executor).awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void should_wakeup() {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                TestFixtures.createQueueSettings().build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueTaskPoller queueTaskPoller = mock(QueueTaskPoller.class);
        ExecutorService executor = mock(ExecutorService.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        when(executor.submit(any(Runnable.class))).thenReturn(mock(Future.class));
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner,
                () -> queueLoop);
        pool.start();
        pool.wakeup();
        verify(queueLoop).doContinue();
    }

    @Test
    public void should_resize_queue_pool() throws InterruptedException {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                TestFixtures.createQueueSettings().withProcessingSettings(
                        TestFixtures.createProcessingSettings().withThreadCount(0).build()).build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueTaskPoller queueTaskPoller = mock(QueueTaskPoller.class);
        ThreadPoolExecutor executor = spy(new ThreadPoolExecutor(
                0,
                Integer.MAX_VALUE,
                1L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new QueueThreadFactory(
                        queueConfig.getLocation(), DEFAULT_SHARD.getShardId())));

        QueueLoop queueLoop = mock(QueueLoop.class);
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner,
                () -> queueLoop);
        pool.start();
        verify(executor, times(0)).submit(any(Runnable.class));
        assertThat(executor.getPoolSize(), equalTo(0));
        pool.resizePool(1);
        assertThat(executor.getPoolSize(), equalTo(1));
        verify(executor, times(1)).submit(any(Runnable.class));
        pool.resizePool(0);
        Thread.sleep(100);
        assertThat(executor.getPoolSize(), equalTo(0));
        verify(executor, times(3)).allowCoreThreadTimeOut(eq(true));
        verify(executor, times(2)).setCorePoolSize(eq(0));
        verify(executor, times(3)).purge();
    }

    @Test
    public void should_resize_queue_pool_when_settings_changed() throws InterruptedException {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                TestFixtures.createQueueSettings().withProcessingSettings(
                        TestFixtures.createProcessingSettings().withThreadCount(0).build()).build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueTaskPoller queueTaskPoller = mock(QueueTaskPoller.class);
        ThreadPoolExecutor executor = spy(new ThreadPoolExecutor(
                0,
                Integer.MAX_VALUE,
                1L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new QueueThreadFactory(
                        queueConfig.getLocation(), DEFAULT_SHARD.getShardId())));

        QueueLoop queueLoop = mock(QueueLoop.class);
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueTaskPoller, executor, queueRunner,
                () -> queueLoop);
        pool.start();
        verify(executor, times(0)).submit(any(Runnable.class));
        assertThat(executor.getPoolSize(), equalTo(0));
        queueConfig.getSettings().getProcessingSettings().setValue(TestFixtures.createProcessingSettings().withThreadCount(1).build());
        assertThat(executor.getPoolSize(), equalTo(1));
        verify(executor, times(1)).submit(any(Runnable.class));
    }
}