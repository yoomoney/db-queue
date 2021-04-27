package ru.yoomoney.tech.dbqueue.internal.processing;

import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.stub.StubDatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.dao.QueueDao;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import java.time.Duration;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class TaskResultHandlerTest {

    @Test
    public void should_reenqueue_task() {
        long taskId = 5L;
        Duration reenqueueDelay = Duration.ofMillis(500L);
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();

        TaskRecord taskRecord = TaskRecord.builder().withId(taskId).build();
        QueueShard queueShard = mock(QueueShard.class);
        QueueDao queueDao = mock(QueueDao.class);
        when(queueShard.getDatabaseAccessLayer()).thenReturn(new StubDatabaseAccessLayer(queueDao));

        ReenqueueRetryStrategy strategy = mock(ReenqueueRetryStrategy.class);

        TaskExecutionResult result = TaskExecutionResult.reenqueue(reenqueueDelay);

        new TaskResultHandler(location, queueShard, strategy).handleResult(taskRecord, result);

        verify(queueShard, times(2)).getDatabaseAccessLayer();
        verify(queueDao).reenqueue(location, taskId, reenqueueDelay);
        verifyNoInteractions(strategy);
    }

    @Test
    public void should_finish_task() {
        long taskId = 5L;
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();

        TaskRecord taskRecord = TaskRecord.builder().withId(taskId).build();
        QueueDao queueDao = mock(QueueDao.class);
        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getDatabaseAccessLayer()).thenReturn(new StubDatabaseAccessLayer(queueDao));

        ReenqueueRetryStrategy strategy = mock(ReenqueueRetryStrategy.class);

        TaskExecutionResult result = TaskExecutionResult.finish();

        new TaskResultHandler(location, queueShard, strategy).handleResult(taskRecord, result);

        verify(queueShard, times(2)).getDatabaseAccessLayer();
        verify(queueDao).deleteTask(location, taskId);
        verifyNoInteractions(strategy);
    }

    @Test
    public void should_fail_task_when_no_delay() {
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();

        TaskRecord taskRecord = TaskRecord.builder().build();
        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getDatabaseAccessLayer()).thenReturn(new StubDatabaseAccessLayer());

        ReenqueueRetryStrategy strategy = mock(ReenqueueRetryStrategy.class);

        TaskExecutionResult result = TaskExecutionResult.fail();

        new TaskResultHandler(location, queueShard, strategy).handleResult(taskRecord, result);

        verifyNoInteractions(queueShard, strategy);
    }

    @Test
    public void should_reenqueue_with_retry_strategy_task() {
        long taskId = 5L;
        QueueLocation location = QueueLocation.builder()
                .withTableName("testTable")
                .withQueueId(new QueueId("testQueue"))
                .build();

        TaskRecord taskRecord = TaskRecord.builder().withId(taskId).build();

        QueueDao queueDao = mock(QueueDao.class);

        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getDatabaseAccessLayer()).thenReturn(new StubDatabaseAccessLayer(queueDao));

        ReenqueueRetryStrategy strategy = mock(ReenqueueRetryStrategy.class);
        when(strategy.calculateDelay(any())).thenReturn(Duration.ofSeconds(10L));

        TaskExecutionResult result = TaskExecutionResult.reenqueue();

        new TaskResultHandler(location, queueShard, strategy).handleResult(taskRecord, result);

        verify(queueShard, times(2)).getDatabaseAccessLayer();
        verify(queueDao).reenqueue(location, taskId, Duration.ofSeconds(10L));
        verify(strategy).calculateDelay(taskRecord);
    }
}