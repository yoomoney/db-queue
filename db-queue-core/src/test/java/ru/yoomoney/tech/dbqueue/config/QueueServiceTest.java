package ru.yoomoney.tech.dbqueue.config;

import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.stub.StubDatabaseAccessLayer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class QueueServiceTest {

    private static final QueueShard<?> DEFAULT_SHARD = new QueueShard<>(new QueueShardId("s1"),
            new StubDatabaseAccessLayer());

    @Test
    public void should_not_do_any_operations_when_queue_is_not_registered() throws Exception {
        QueueConsumer<?> consumer = mock(QueueConsumer.class);
        QueueId queueId = new QueueId("test");
        when(consumer.getQueueConfig()).thenReturn(new QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                        .withQueueId(queueId).build(),
                QueueSettings.builder()
                        .withNoTaskTimeout(Duration.ZERO)
                        .withThreadCount(0)
                        .withBetweenTaskTimeout(Duration.ZERO).build()));
        QueueExecutionPool queueExecutionPool = mock(QueueExecutionPool.class);
        QueueService queueService = new QueueService(Collections.singletonList(DEFAULT_SHARD),
                ((queueShard, queueConsumer) -> queueExecutionPool));
        List<String> errorMessages = new ArrayList<>();

        queueService.start();
        try {
            queueService.start(queueId);
        } catch (IllegalArgumentException exc) {
            errorMessages.add(exc.getMessage());
        }
        queueService.pause();
        try {
            queueService.pause(queueId);
        } catch (IllegalArgumentException exc) {
            errorMessages.add(exc.getMessage());
        }
        queueService.unpause();
        try {
            queueService.unpause(queueId);
        } catch (IllegalArgumentException exc) {
            errorMessages.add(exc.getMessage());
        }
        queueService.isPaused();
        try {
            queueService.isPaused(queueId);
        } catch (IllegalArgumentException exc) {
            errorMessages.add(exc.getMessage());
        }
        queueService.isShutdown();

        try {
            queueService.isShutdown(queueId);
        } catch (IllegalArgumentException exc) {
            errorMessages.add(exc.getMessage());
        }
        queueService.isTerminated();
        try {
            queueService.isTerminated(queueId);
        } catch (IllegalArgumentException exc) {
            errorMessages.add(exc.getMessage());
        }
        queueService.awaitTermination(Duration.ZERO);
        try {
            queueService.awaitTermination(queueId, Duration.ZERO);
        } catch (IllegalArgumentException exc) {
            errorMessages.add(exc.getMessage());
        }
        try {
            queueService.wakeup(queueId, DEFAULT_SHARD.getShardId());
        } catch (IllegalArgumentException exc) {
            errorMessages.add(exc.getMessage());
        }
        try {
            queueService.resizePool(queueId, DEFAULT_SHARD.getShardId(), 1);
        } catch (IllegalArgumentException exc) {
            errorMessages.add(exc.getMessage());
        }

        verifyZeroInteractions(queueExecutionPool);
        assertThat(errorMessages.toString(), equalTo(
                "[cannot invoke start, queue is not registered: queueId=test, " +
                        "cannot invoke pause, queue is not registered: queueId=test, " +
                        "cannot invoke unpause, queue is not registered: queueId=test, " +
                        "cannot invoke isPaused, queue is not registered: queueId=test, " +
                        "cannot invoke isShutdown, queue is not registered: queueId=test, " +
                        "cannot invoke isTerminated, queue is not registered: queueId=test, " +
                        "cannot invoke awaitTermination, queue is not registered: queueId=test, " +
                        "cannot invoke wakeup, queue is not registered: queueId=test, " +
                        "cannot invoke resizePool, queue is not registered: queueId=test" +
                        "]"));
    }

    @Test
    public void should_not_register_queue_when_already_registered() throws Exception {

        QueueConsumer<?> consumer = mock(QueueConsumer.class);
        when(consumer.getQueueConfig()).thenReturn(new QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                        .withQueueId(new QueueId("queue1")).build(),
                QueueSettings.builder()
                        .withNoTaskTimeout(Duration.ZERO)
                        .withThreadCount(1)
                        .withBetweenTaskTimeout(Duration.ZERO).build()));

        QueueService queueService = new QueueService(Collections.singletonList(DEFAULT_SHARD),
                mock(ThreadLifecycleListener.class), mock(TaskLifecycleListener.class));
        assertTrue(queueService.registerQueue(consumer));
        assertFalse(queueService.registerQueue(consumer));
    }

    @Test
    public void should_work_with_more_than_one_queue() {
        QueueConsumer<?> consumer1 = mock(QueueConsumer.class);
        QueueId queueId1 = new QueueId("queue1");
        when(consumer1.getQueueConfig()).thenReturn(new QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                        .withQueueId(queueId1).build(),
                QueueSettings.builder()
                        .withNoTaskTimeout(Duration.ZERO)
                        .withThreadCount(1)
                        .withBetweenTaskTimeout(Duration.ZERO).build()));
        QueueConsumer<?> consumer2 = mock(QueueConsumer.class);
        QueueId queueId2 = new QueueId("queue2");
        when(consumer2.getQueueConfig()).thenReturn(new QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                        .withQueueId(queueId2).build(),
                QueueSettings.builder()
                        .withNoTaskTimeout(Duration.ZERO)
                        .withThreadCount(1)
                        .withBetweenTaskTimeout(Duration.ZERO).build()));
        QueueExecutionPool queueExecutionPool1 = mock(QueueExecutionPool.class);
        when(queueExecutionPool1.isPaused()).thenReturn(true);
        when(queueExecutionPool1.isShutdown()).thenReturn(true);
        when(queueExecutionPool1.isTerminated()).thenReturn(true);
        QueueExecutionPool queueExecutionPool2 = mock(QueueExecutionPool.class);
        when(queueExecutionPool2.isPaused()).thenReturn(true);
        when(queueExecutionPool2.isShutdown()).thenReturn(true);
        when(queueExecutionPool2.isTerminated()).thenReturn(true);

        QueueService queueService = new QueueService(Collections.singletonList(DEFAULT_SHARD),
                (shard, queueConsumer) -> {
                    if (queueConsumer.getQueueConfig().getLocation().getQueueId().equals(queueId1)) {
                        return queueExecutionPool1;
                    } else if (queueConsumer.getQueueConfig().getLocation().getQueueId().equals(queueId2)) {
                        return queueExecutionPool2;
                    }
                    throw new IllegalArgumentException("unknown consumer");
                });
        assertTrue(queueService.registerQueue(consumer1));
        assertTrue(queueService.registerQueue(consumer2));
        queueService.start();
        queueService.start(queueId1);
        queueService.pause();
        queueService.pause(queueId1);
        queueService.unpause();
        queueService.unpause(queueId1);
        queueService.isPaused();
        queueService.isPaused(queueId1);
        queueService.isShutdown();
        queueService.isShutdown(queueId1);
        queueService.isTerminated();
        queueService.isTerminated(queueId1);
        queueService.wakeup(queueId1, DEFAULT_SHARD.getShardId());

        verify(queueExecutionPool1, times(2)).start();
        verify(queueExecutionPool1, times(2)).pause();
        verify(queueExecutionPool1, times(2)).unpause();
        verify(queueExecutionPool1, times(2)).isPaused();
        verify(queueExecutionPool1, times(2)).isShutdown();
        verify(queueExecutionPool1, times(2)).isTerminated();
        verify(queueExecutionPool1, times(1)).wakeup();

        verify(queueExecutionPool2, times(1)).start();
        verify(queueExecutionPool2, times(1)).pause();
        verify(queueExecutionPool2, times(1)).unpause();
        verify(queueExecutionPool2, times(1)).isPaused();
        verify(queueExecutionPool2, times(1)).isShutdown();
        verify(queueExecutionPool2, times(1)).isTerminated();

        verifyNoMoreInteractions(queueExecutionPool1);
        verifyNoMoreInteractions(queueExecutionPool2);
    }

    @Test
    public void should_work_with_more_than_one_shard() {
        QueueConsumer<?> consumer1 = mock(QueueConsumer.class);
        QueueId queueId1 = new QueueId("queue1");
        when(consumer1.getQueueConfig()).thenReturn(new QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                        .withQueueId(queueId1).build(),
                QueueSettings.builder()
                        .withNoTaskTimeout(Duration.ZERO)
                        .withThreadCount(1)
                        .withBetweenTaskTimeout(Duration.ZERO).build()));

        QueueExecutionPool queueExecutionPool1 = mock(QueueExecutionPool.class);
        when(queueExecutionPool1.isPaused()).thenReturn(true);
        when(queueExecutionPool1.isShutdown()).thenReturn(true);
        when(queueExecutionPool1.isTerminated()).thenReturn(true);
        QueueExecutionPool queueExecutionPool2 = mock(QueueExecutionPool.class);
        when(queueExecutionPool2.isPaused()).thenReturn(true);
        when(queueExecutionPool2.isShutdown()).thenReturn(true);
        when(queueExecutionPool2.isTerminated()).thenReturn(true);

        QueueShard<?> shard2 = new QueueShard<>(new QueueShardId("s2"),
                new StubDatabaseAccessLayer());

        QueueService queueService = new QueueService(Arrays.asList(DEFAULT_SHARD, shard2),
                (shard, queueConsumer) -> {
                    if (shard.getShardId().equals(DEFAULT_SHARD.getShardId())) {
                        return queueExecutionPool1;
                    } else if (shard.getShardId().equals(shard2.getShardId())) {
                        return queueExecutionPool2;
                    }
                    throw new IllegalArgumentException("unknown consumer");
                });
        assertTrue(queueService.registerQueue(consumer1));
        queueService.start();
        queueService.pause();
        queueService.unpause();
        queueService.isPaused();
        queueService.isShutdown();
        queueService.isTerminated();
        queueService.wakeup(queueId1, DEFAULT_SHARD.getShardId());
        queueService.wakeup(queueId1, shard2.getShardId());

        verify(queueExecutionPool1, times(1)).start();
        verify(queueExecutionPool1, times(1)).pause();
        verify(queueExecutionPool1, times(1)).unpause();
        verify(queueExecutionPool1, times(1)).isPaused();
        verify(queueExecutionPool1, times(1)).isShutdown();
        verify(queueExecutionPool1, times(1)).isTerminated();
        verify(queueExecutionPool1, times(1)).wakeup();

        verify(queueExecutionPool2, times(1)).start();
        verify(queueExecutionPool2, times(1)).pause();
        verify(queueExecutionPool2, times(1)).unpause();
        verify(queueExecutionPool2, times(1)).isPaused();
        verify(queueExecutionPool2, times(1)).isShutdown();
        verify(queueExecutionPool2, times(1)).isTerminated();
        verify(queueExecutionPool2, times(1)).wakeup();

        verifyNoMoreInteractions(queueExecutionPool1);
        verifyNoMoreInteractions(queueExecutionPool2);
    }

    @Test
    public void should_await_queue_termination() {
        QueueConsumer<?> consumer = mock(QueueConsumer.class);
        QueueId queueId = new QueueId("queue1");
        when(consumer.getQueueConfig()).thenReturn(new QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                        .withQueueId(queueId).build(),
                QueueSettings.builder()
                        .withNoTaskTimeout(Duration.ZERO)
                        .withThreadCount(1)
                        .withBetweenTaskTimeout(Duration.ZERO).build()));
        QueueExecutionPool queueExecutionPool = mock(QueueExecutionPool.class);
        when(queueExecutionPool.isTerminated()).thenReturn(false);
        when(queueExecutionPool.getQueueShardId()).thenReturn(DEFAULT_SHARD.getShardId());
        QueueService queueService = new QueueService(Arrays.asList(DEFAULT_SHARD),
                (shard, queueConsumer) -> queueExecutionPool);

        assertTrue(queueService.registerQueue(consumer));
        assertThat(queueService.awaitTermination(queueId, Duration.ofMinutes(1)),
                equalTo(Collections.singletonList(DEFAULT_SHARD.getShardId())));
        verify(queueExecutionPool).awaitTermination(Duration.ofMinutes(1));
        verify(queueExecutionPool).isTerminated();
    }

    @Test
    public void should_await_termination() {
        QueueConsumer<?> consumer = mock(QueueConsumer.class);
        QueueId queueId = new QueueId("queue1");
        when(consumer.getQueueConfig()).thenReturn(new QueueConfig(
                QueueLocation.builder().withTableName("testTable")
                        .withQueueId(queueId).build(),
                QueueSettings.builder()
                        .withNoTaskTimeout(Duration.ZERO)
                        .withThreadCount(1)
                        .withBetweenTaskTimeout(Duration.ZERO).build()));
        QueueExecutionPool queueExecutionPool = mock(QueueExecutionPool.class);
        when(queueExecutionPool.isTerminated()).thenReturn(false);
        when(queueExecutionPool.getQueueShardId()).thenReturn(DEFAULT_SHARD.getShardId());
        QueueService queueService = new QueueService(Arrays.asList(DEFAULT_SHARD),
                (shard, queueConsumer) -> queueExecutionPool);

        assertTrue(queueService.registerQueue(consumer));
        assertThat(queueService.awaitTermination(Duration.ofMinutes(1)),
                equalTo(Collections.singletonList(queueId)));
        verify(queueExecutionPool).awaitTermination(Duration.ofMinutes(1));
        verify(queueExecutionPool, times(2)).isTerminated();
    }
}
