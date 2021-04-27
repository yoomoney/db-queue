package ru.yoomoney.tech.dbqueue.config;

import org.junit.Test;
import ru.yoomoney.tech.dbqueue.internal.processing.QueueLoop;
import ru.yoomoney.tech.dbqueue.internal.runner.QueueRunner;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.stub.NoopQueueConsumer;
import ru.yoomoney.tech.dbqueue.stub.StringQueueConsumer;
import ru.yoomoney.tech.dbqueue.stub.StubDatabaseAccessLayer;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
                QueueSettings.builder()
                        .withNoTaskTimeout(Duration.ZERO)
                        .withThreadCount(2)
                        .withBetweenTaskTimeout(Duration.ZERO).build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueLoop, new DirectExecutor(), queueRunner);
        pool.start();
        verify(queueLoop, times(2)).start(DEFAULT_SHARD.getShardId(), consumer, queueRunner);
    }

    @Test
    public void should_shutdown() {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                QueueSettings.builder().withNoTaskTimeout(Duration.ZERO).withBetweenTaskTimeout(Duration.ZERO).build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        ExecutorService executor = mock(ExecutorService.class);
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueLoop, executor, queueRunner);
        pool.shutdown();
        verify(executor).shutdownNow();
    }

    @Test
    public void should_pause() {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                QueueSettings.builder().withNoTaskTimeout(Duration.ZERO).withBetweenTaskTimeout(Duration.ZERO).build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        ExecutorService executor = mock(ExecutorService.class);
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueLoop, executor, queueRunner);
        pool.pause();
        verify(queueLoop).pause();
    }

    @Test
    public void should_invoke_ispaused() {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                QueueSettings.builder().withNoTaskTimeout(Duration.ZERO).withBetweenTaskTimeout(Duration.ZERO).build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        ExecutorService executor = mock(ExecutorService.class);
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueLoop, executor, queueRunner);
        pool.isPaused();
        verify(queueLoop).isPaused();
    }

    @Test
    public void should_invoke_isterminated() {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                QueueSettings.builder().withNoTaskTimeout(Duration.ZERO).withBetweenTaskTimeout(Duration.ZERO).build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        ExecutorService executor = mock(ExecutorService.class);
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueLoop, executor, queueRunner);
        pool.isTerminated();
        verify(executor).isTerminated();
    }

    @Test
    public void should_invoke_isshutdown() {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                QueueSettings.builder().withNoTaskTimeout(Duration.ZERO).withBetweenTaskTimeout(Duration.ZERO).build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        ExecutorService executor = mock(ExecutorService.class);
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueLoop, executor, queueRunner);
        pool.isShutdown();
        verify(executor).isShutdown();
    }

    public void should_await_termination() throws InterruptedException {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                QueueSettings.builder().withNoTaskTimeout(Duration.ZERO).withBetweenTaskTimeout(Duration.ZERO).build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        ExecutorService executor = mock(ExecutorService.class);
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueLoop, executor, queueRunner);
        pool.awaitTermination(Duration.ofSeconds(10));
        verify(executor).awaitTermination(10, TimeUnit.SECONDS);
    }

    public void should_wakeup() throws InterruptedException {
        QueueConfig queueConfig = new QueueConfig(
                QueueLocation.builder().withTableName("testTable").withQueueId(new QueueId("queue1")).build(),
                QueueSettings.builder().withNoTaskTimeout(Duration.ZERO).withBetweenTaskTimeout(Duration.ZERO).build());
        StringQueueConsumer consumer = new NoopQueueConsumer(queueConfig);
        QueueRunner queueRunner = mock(QueueRunner.class);
        QueueLoop queueLoop = mock(QueueLoop.class);
        ExecutorService executor = mock(ExecutorService.class);
        QueueExecutionPool pool = new QueueExecutionPool(consumer, DEFAULT_SHARD, queueLoop, executor, queueRunner);
        pool.wakeup();
        verify(queueLoop).wakeup();
    }
}