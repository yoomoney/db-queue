package ru.yandex.money.common.dbqueue.internal.processing;

import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.config.QueueShard;
import ru.yandex.money.common.dbqueue.config.QueueShardId;
import ru.yandex.money.common.dbqueue.config.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.internal.pick.QueuePickTaskDao;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.stub.FakeMillisTimeProvider;
import ru.yandex.money.common.dbqueue.stub.FakeTransactionTemplate;

import java.time.Duration;
import java.util.Arrays;

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
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        QueueShardId shardId = new QueueShardId("s1");
        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getShardId()).thenReturn(shardId);
        FakeTransactionTemplate transactionTemplate = spy(new FakeTransactionTemplate());
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build()));
        QueuePickTaskDao pickTaskDao = mock(QueuePickTaskDao.class);
        when(queueShard.getTransactionTemplate()).thenReturn(transactionTemplate);
        TaskRecord taskRecord = TaskRecord.builder().build();
        when(pickTaskDao.pickTask(location)).thenReturn(taskRecord);
        when(queueShard.getShardId()).thenReturn(shardId);
        TaskLifecycleListener listener = mock(TaskLifecycleListener.class);
        FakeMillisTimeProvider millisTimeProvider = spy(new FakeMillisTimeProvider(Arrays.asList(3L, 5L)));

        TaskRecord pickedTask = new TaskPicker(queueShard, listener, millisTimeProvider, pickTaskDao).pickTask(queueConsumer);

        assertThat(pickedTask, equalTo(taskRecord));

        verify(millisTimeProvider, times(2)).getMillis();
        verify(queueShard).getTransactionTemplate();
        verify(pickTaskDao).pickTask(location);
        verify(listener).picked(shardId, location, taskRecord, 2L);
    }

    @Test
    public void should_not_notify_when_task_not_picked() throws Exception {
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        QueueShard queueShard = mock(QueueShard.class);
        FakeTransactionTemplate transactionTemplate = spy(new FakeTransactionTemplate());
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build()));
        QueuePickTaskDao pickTaskDao = mock(QueuePickTaskDao.class);
        when(queueShard.getTransactionTemplate()).thenReturn(transactionTemplate);
        when(pickTaskDao.pickTask(location)).thenReturn(null);
        TaskLifecycleListener listener = mock(TaskLifecycleListener.class);
        FakeMillisTimeProvider millisTimeProvider = spy(new FakeMillisTimeProvider(Arrays.asList(3L, 5L)));

        TaskRecord pickedTask = new TaskPicker(queueShard, listener, millisTimeProvider, pickTaskDao).pickTask(queueConsumer);

        assertThat(pickedTask, equalTo(null));

        verify(millisTimeProvider).getMillis();
        verify(queueShard).getTransactionTemplate();
        verify(pickTaskDao).pickTask(location);
        verifyZeroInteractions(listener);
    }

    @Test(expected = IllegalStateException.class)
    public void should_not_catch_exception() throws Exception {
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        QueueShard queueShard = mock(QueueShard.class);
        FakeTransactionTemplate transactionTemplate = spy(new FakeTransactionTemplate());
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build()));
        QueuePickTaskDao pickTaskDao = mock(QueuePickTaskDao.class);
        when(queueShard.getTransactionTemplate()).thenReturn(transactionTemplate);
        when(pickTaskDao.pickTask(location)).thenThrow(new IllegalStateException("fail"));
        TaskLifecycleListener listener = mock(TaskLifecycleListener.class);
        FakeMillisTimeProvider millisTimeProvider = spy(new FakeMillisTimeProvider(Arrays.asList(3L, 5L)));

        TaskRecord pickedTask = new TaskPicker(queueShard, listener, millisTimeProvider, pickTaskDao).pickTask(queueConsumer);

        assertThat(pickedTask, equalTo(null));

        verify(millisTimeProvider).getMillis();
        verify(queueShard).getTransactionTemplate();
        verify(pickTaskDao).pickTask(location);
        verifyZeroInteractions(listener);
    }
}