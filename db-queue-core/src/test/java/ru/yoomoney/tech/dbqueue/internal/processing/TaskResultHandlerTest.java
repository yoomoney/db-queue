package ru.yoomoney.tech.dbqueue.internal.processing;

import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.dao.QueueDao;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueRetryType;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueSettings;
import ru.yoomoney.tech.dbqueue.stub.StubDatabaseAccessLayer;

import java.time.Duration;

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

        TaskExecutionResult result = TaskExecutionResult.reenqueue(reenqueueDelay);
        ReenqueueSettings reenqueueSettings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.MANUAL).build();
        new TaskResultHandler(location, queueShard, reenqueueSettings).handleResult(taskRecord, result);

        verify(queueShard, times(2)).getDatabaseAccessLayer();
        verify(queueDao).reenqueue(location, taskId, reenqueueDelay);
    }

    @Test
    public void should_reenqueue_task_with_new_settings() {
        long taskId = 5L;
        Duration reenqueueDelay = Duration.ofMillis(500L);
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();

        TaskRecord taskRecord = TaskRecord.builder().withId(taskId).build();
        QueueShard queueShard = mock(QueueShard.class);
        QueueDao queueDao = mock(QueueDao.class);
        when(queueShard.getDatabaseAccessLayer()).thenReturn(new StubDatabaseAccessLayer(queueDao));

        ReenqueueSettings reenqueueSettings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.MANUAL).build();
        new TaskResultHandler(location, queueShard, reenqueueSettings).handleResult(taskRecord,
                TaskExecutionResult.reenqueue(reenqueueDelay));

        verify(queueShard, times(2)).getDatabaseAccessLayer();
        verify(queueDao).reenqueue(location, taskId, reenqueueDelay);


        Duration newReenqueueDelay = Duration.ofMillis(1000L);
        reenqueueSettings.setValue(ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.FIXED)
                .withFixedDelay(newReenqueueDelay).build());
        new TaskResultHandler(location, queueShard, reenqueueSettings).handleResult(taskRecord,
                TaskExecutionResult.reenqueue());
        verify(queueDao).reenqueue(location, taskId, newReenqueueDelay);
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

        TaskExecutionResult result = TaskExecutionResult.finish();

        ReenqueueSettings reenqueueSettings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.MANUAL).build();
        new TaskResultHandler(location, queueShard, reenqueueSettings)
                .handleResult(taskRecord, result);

        verify(queueShard, times(2)).getDatabaseAccessLayer();
        verify(queueDao).deleteTask(location, taskId);
    }

    @Test
    public void should_fail_task_when_no_delay() {
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();

        TaskRecord taskRecord = TaskRecord.builder().build();
        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getDatabaseAccessLayer()).thenReturn(new StubDatabaseAccessLayer());

        TaskExecutionResult result = TaskExecutionResult.fail();

        ReenqueueSettings reenqueueSettings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.MANUAL).build();
        new TaskResultHandler(location, queueShard, reenqueueSettings).handleResult(taskRecord, result);

        verifyNoInteractions(queueShard);
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


        TaskExecutionResult result = TaskExecutionResult.reenqueue();
        ReenqueueSettings reenqueueSettings = ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.FIXED)
                .withFixedDelay(Duration.ofSeconds(10L)).build();
        new TaskResultHandler(location, queueShard, reenqueueSettings).handleResult(taskRecord, result);

        verify(queueShard, times(2)).getDatabaseAccessLayer();
        verify(queueDao).reenqueue(location, taskId, Duration.ofSeconds(10L));
    }
}