package ru.yandex.money.common.dbqueue.internal.runner;

import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueShard;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.internal.QueueProcessingStatus;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.stub.FakeTransactionTemplate;

import java.time.Duration;
import java.time.ZonedDateTime;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class QueueRunnerInTransactionTest {

    private static final QueueLocation testLocation1 =
            QueueLocation.builder().withTableName("queue_test")
                    .withQueueId(new QueueId("test_queue1")).build();

    @Test
    public void should_wait_notasktimeout_when_no_task_found() throws Exception {
        Duration betweenTaskTimeout = Duration.ofHours(1L);
        Duration noTaskTimeout = Duration.ofMillis(5L);

        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        TaskPicker taskPicker = mock(TaskPicker.class);
        when(taskPicker.pickTask(queueConsumer)).thenReturn(null);
        TaskProcessor taskProcessor = mock(TaskProcessor.class);
        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getTransactionTemplate()).thenReturn(new FakeTransactionTemplate());

        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(testLocation1,
                QueueSettings.builder().withBetweenTaskTimeout(betweenTaskTimeout).withNoTaskTimeout(noTaskTimeout).build()));
        QueueProcessingStatus status = new QueueRunnerInTransaction(taskPicker, taskProcessor, queueShard).runQueue(queueConsumer);

        assertThat(status, equalTo(QueueProcessingStatus.SKIPPED));

        verify(queueShard).getTransactionTemplate();
        verify(taskPicker).pickTask(queueConsumer);
        verifyZeroInteractions(taskProcessor);
    }

    @Test
    public void should_wait_betweentasktimeout_when_task_found() throws Exception {
        Duration betweenTaskTimeout = Duration.ofHours(1L);
        Duration noTaskTimeout = Duration.ofMillis(5L);

        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        TaskPicker taskPicker = mock(TaskPicker.class);
        TaskRecord taskRecord = new TaskRecord(0, null, 0, ZonedDateTime.now(),
                ZonedDateTime.now(), null, null);
        when(taskPicker.pickTask(queueConsumer)).thenReturn(taskRecord);
        TaskProcessor taskProcessor = mock(TaskProcessor.class);
        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getTransactionTemplate()).thenReturn(new FakeTransactionTemplate());


        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(testLocation1,
                QueueSettings.builder().withBetweenTaskTimeout(betweenTaskTimeout).withNoTaskTimeout(noTaskTimeout).build()));
        QueueProcessingStatus queueProcessingStatus = new QueueRunnerInTransaction(taskPicker, taskProcessor, queueShard).runQueue(queueConsumer);

        assertThat(queueProcessingStatus, equalTo(QueueProcessingStatus.PROCESSED));

        verify(queueShard).getTransactionTemplate();
        verify(taskPicker).pickTask(queueConsumer);
        verify(taskProcessor).processTask(queueConsumer, taskRecord);
    }
}