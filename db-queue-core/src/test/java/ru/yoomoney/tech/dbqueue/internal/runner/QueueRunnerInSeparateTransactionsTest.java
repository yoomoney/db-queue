package ru.yoomoney.tech.dbqueue.internal.runner;

import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.internal.processing.QueueProcessingStatus;
import ru.yoomoney.tech.dbqueue.internal.processing.TaskPicker;
import ru.yoomoney.tech.dbqueue.internal.processing.TaskProcessor;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.stub.TestFixtures;

import java.time.Duration;

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
public class QueueRunnerInSeparateTransactionsTest {

    private static final QueueLocation testLocation1 =
            QueueLocation.builder().withTableName("queue_test")
                    .withQueueId(new QueueId("test_queue1")).build();

    @Test
    public void should_wait_notasktimeout_when_no_task_found() {
        Duration betweenTaskTimeout = Duration.ofHours(1L);
        Duration noTaskTimeout = Duration.ofMillis(5L);

        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        TaskPicker taskPicker = mock(TaskPicker.class);
        when(taskPicker.pickTask()).thenReturn(null);
        TaskProcessor taskProcessor = mock(TaskProcessor.class);

        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(testLocation1,
                TestFixtures.createQueueSettings().withPollSettings(TestFixtures.createPollSettings()
                        .withBetweenTaskTimeout(betweenTaskTimeout).withNoTaskTimeout(noTaskTimeout).build()).build()));
        QueueProcessingStatus status = new QueueRunnerInSeparateTransactions(taskPicker, taskProcessor).runQueue(queueConsumer);

        assertThat(status, equalTo(QueueProcessingStatus.SKIPPED));

        verify(taskPicker).pickTask();
        verifyZeroInteractions(taskProcessor);
    }

    @Test
    public void should_wait_betweentasktimeout_when_task_found() {
        Duration betweenTaskTimeout = Duration.ofHours(1L);
        Duration noTaskTimeout = Duration.ofMillis(5L);

        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        TaskPicker taskPicker = mock(TaskPicker.class);
        TaskRecord taskRecord = TaskRecord.builder().build();
        when(taskPicker.pickTask()).thenReturn(taskRecord);
        TaskProcessor taskProcessor = mock(TaskProcessor.class);


        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(testLocation1,
                TestFixtures.createQueueSettings().withPollSettings(TestFixtures.createPollSettings()
                        .withBetweenTaskTimeout(betweenTaskTimeout).withNoTaskTimeout(noTaskTimeout).build()).build()));
        QueueProcessingStatus status = new QueueRunnerInSeparateTransactions(taskPicker, taskProcessor).runQueue(queueConsumer);

        assertThat(status, equalTo(QueueProcessingStatus.PROCESSED));

        verify(taskPicker).pickTask();
        verify(taskProcessor).processTask(queueConsumer, taskRecord);
    }

}