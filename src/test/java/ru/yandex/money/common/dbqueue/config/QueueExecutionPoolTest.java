package ru.yandex.money.common.dbqueue.config;

import example.StringQueueConsumer;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yandex.money.common.dbqueue.internal.processing.QueueLoop;
import ru.yandex.money.common.dbqueue.internal.runner.QueueRunner;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.stub.NoopQueueConsumer;

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

    private static QueueShard DEFAULT_SHARD = new QueueShard(DatabaseDialect.POSTGRESQL, QueueTableSchema.builder().build(),
            new QueueShardId("s1"), mock(JdbcOperations.class), mock(TransactionOperations.class));

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