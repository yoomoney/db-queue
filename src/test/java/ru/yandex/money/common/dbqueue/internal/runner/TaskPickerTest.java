package ru.yandex.money.common.dbqueue.internal.runner;

import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.Queue;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.stub.FakeMillisTimeProvider;
import ru.yandex.money.common.dbqueue.stub.FakeTransactionTemplate;

import java.time.Duration;
import java.time.ZonedDateTime;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class TaskPickerTest {

    @Test
    public void should_successfully_pick_task() throws Exception {
        QueueLocation location = new QueueLocation("testTable", "testQueue");
        QueueShardId shardId = new QueueShardId("s1");

        RetryTaskStrategy retryTaskStrategy = mock(RetryTaskStrategy.class);
        FakeTransactionTemplate transactionTemplate = spy(new FakeTransactionTemplate());
        Queue queue = mock(Queue.class);
        when(queue.getQueueConfig()).thenReturn(new QueueConfig(location,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build()));
        PickTaskDao pickTaskDao = mock(PickTaskDao.class);
        when(pickTaskDao.getTransactionTemplate()).thenReturn(transactionTemplate);
        TaskRecord taskRecord = new TaskRecord(0, null, 0, ZonedDateTime.now(),
                ZonedDateTime.now(), null, null);
        when(pickTaskDao.pickTask(location, retryTaskStrategy)).thenReturn(taskRecord);
        when(pickTaskDao.getShardId()).thenReturn(shardId);
        TaskLifecycleListener listener = mock(TaskLifecycleListener.class);
        FakeMillisTimeProvider millisTimeProvider = spy(new FakeMillisTimeProvider(3L, 5L));

        TaskRecord pickedTask = new TaskPicker(pickTaskDao, listener, millisTimeProvider, retryTaskStrategy).pickTask(queue);

        assertThat(pickedTask, equalTo(taskRecord));

        verify(millisTimeProvider, times(2)).getMillis();
        verify(pickTaskDao).getTransactionTemplate();
        verify(pickTaskDao).pickTask(location, retryTaskStrategy);
        verify(listener).picked(shardId, location, taskRecord, 2L);
    }

    @Test
    public void should_not_notify_when_task_not_picked() throws Exception {
        QueueLocation location = new QueueLocation("testTable", "testQueue");

        RetryTaskStrategy retryTaskStrategy = mock(RetryTaskStrategy.class);
        FakeTransactionTemplate transactionTemplate = spy(new FakeTransactionTemplate());
        Queue queue = mock(Queue.class);
        when(queue.getQueueConfig()).thenReturn(new QueueConfig(location,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build()));
        PickTaskDao pickTaskDao = mock(PickTaskDao.class);
        when(pickTaskDao.getTransactionTemplate()).thenReturn(transactionTemplate);
        when(pickTaskDao.pickTask(location, retryTaskStrategy)).thenReturn(null);
        TaskLifecycleListener listener = mock(TaskLifecycleListener.class);
        FakeMillisTimeProvider millisTimeProvider = spy(new FakeMillisTimeProvider(3L, 5L));

        TaskRecord pickedTask = new TaskPicker(pickTaskDao, listener, millisTimeProvider, retryTaskStrategy).pickTask(queue);

        assertThat(pickedTask, equalTo(null));

        verify(millisTimeProvider).getMillis();
        verify(pickTaskDao).getTransactionTemplate();
        verify(pickTaskDao).pickTask(location, retryTaskStrategy);
        verifyZeroInteractions(listener);
    }

    @Test(expected = IllegalStateException.class)
    public void should_not_catch_exception() throws Exception {
        QueueLocation location = new QueueLocation("testTable", "testQueue");

        RetryTaskStrategy retryTaskStrategy = mock(RetryTaskStrategy.class);
        FakeTransactionTemplate transactionTemplate = spy(new FakeTransactionTemplate());
        Queue queue = mock(Queue.class);
        when(queue.getQueueConfig()).thenReturn(new QueueConfig(location,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build()));
        PickTaskDao pickTaskDao = mock(PickTaskDao.class);
        when(pickTaskDao.getTransactionTemplate()).thenReturn(transactionTemplate);
        when(pickTaskDao.pickTask(location, retryTaskStrategy)).thenThrow(new IllegalStateException("fail"));
        TaskLifecycleListener listener = mock(TaskLifecycleListener.class);
        FakeMillisTimeProvider millisTimeProvider = spy(new FakeMillisTimeProvider(3L, 5L));

        TaskRecord pickedTask = new TaskPicker(pickTaskDao, listener, millisTimeProvider, retryTaskStrategy).pickTask(queue);

        assertThat(pickedTask, equalTo(null));

        verify(millisTimeProvider).getMillis();
        verify(pickTaskDao).getTransactionTemplate();
        verify(pickTaskDao).pickTask(location, retryTaskStrategy);
        verifyZeroInteractions(listener);
    }
}