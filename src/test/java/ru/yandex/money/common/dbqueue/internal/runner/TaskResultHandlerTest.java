package ru.yandex.money.common.dbqueue.internal.runner;

import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.QueueShard;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.stub.FakeTransactionTemplate;

import java.time.Duration;
import java.time.ZonedDateTime;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class TaskResultHandlerTest {

    @Test
    public void should_reenqueue_task() throws Exception {
        long taskId = 5L;
        Duration reenqueueDelay = Duration.ofMillis(500L);
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();

        TaskRecord taskRecord = new TaskRecord(taskId, null, 0, ZonedDateTime.now(),
                ZonedDateTime.now(), null, null);
        QueueShard queueShard = mock(QueueShard.class);
        QueueDao queueDao = mock(QueueDao.class);
        when(queueShard.getTransactionTemplate()).thenReturn(new FakeTransactionTemplate());
        when(queueShard.getQueueDao()).thenReturn(queueDao);
        TaskExecutionResult result = TaskExecutionResult.reenqueue(reenqueueDelay);

        new TaskResultHandler(location, queueShard).handleResult(taskRecord, result);

        verify(queueShard).getTransactionTemplate();
        verify(queueDao).reenqueue(location, taskId, reenqueueDelay);
    }

    @Test
    public void should_finish_task() throws Exception {
        long taskId = 5L;
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();

        TaskRecord taskRecord = new TaskRecord(taskId, null, 0, ZonedDateTime.now(),
                ZonedDateTime.now(), null, null);
        QueueDao queueDao = mock(QueueDao.class);
        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getTransactionTemplate()).thenReturn(new FakeTransactionTemplate());
        when(queueShard.getQueueDao()).thenReturn(queueDao);
        TaskExecutionResult result = TaskExecutionResult.finish();

        new TaskResultHandler(location, queueShard).handleResult(taskRecord, result);

        verify(queueShard).getTransactionTemplate();
        verify(queueDao).deleteTask(location, taskId);
    }

    @Test
    public void should_fail_task_when_no_delay() throws Exception {
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();

        TaskRecord taskRecord = new TaskRecord(0, null, 0, ZonedDateTime.now(),
                ZonedDateTime.now(), null, null);
        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getTransactionTemplate()).thenReturn(new FakeTransactionTemplate());
        TaskExecutionResult result = TaskExecutionResult.fail();

        new TaskResultHandler(location, queueShard).handleResult(taskRecord, result);

        verifyZeroInteractions(queueShard);
    }
}