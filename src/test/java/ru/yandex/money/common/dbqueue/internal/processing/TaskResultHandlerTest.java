package ru.yandex.money.common.dbqueue.internal.processing;

import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.config.QueueShard;
import ru.yandex.money.common.dbqueue.dao.PostgresQueueDao;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.stub.FakeTransactionTemplate;

import java.time.Duration;

import static org.mockito.Mockito.any;
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
    public void should_reenqueue_task() {
        long taskId = 5L;
        Duration reenqueueDelay = Duration.ofMillis(500L);
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();

        TaskRecord taskRecord = TaskRecord.builder().withId(taskId).build();
        QueueShard queueShard = mock(QueueShard.class);
        PostgresQueueDao queueDao = mock(PostgresQueueDao.class);
        when(queueShard.getTransactionTemplate()).thenReturn(new FakeTransactionTemplate());
        when(queueShard.getQueueDao()).thenReturn(queueDao);

        ReenqueueRetryStrategy strategy = mock(ReenqueueRetryStrategy.class);

        TaskExecutionResult result = TaskExecutionResult.reenqueue(reenqueueDelay);

        new TaskResultHandler(location, queueShard, strategy).handleResult(taskRecord, result);

        verify(queueShard).getTransactionTemplate();
        verify(queueDao).reenqueue(location, taskId, reenqueueDelay);
        verifyZeroInteractions(strategy);
    }

    @Test
    public void should_finish_task() {
        long taskId = 5L;
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();

        TaskRecord taskRecord = TaskRecord.builder().withId(taskId).build();
        PostgresQueueDao queueDao = mock(PostgresQueueDao.class);
        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getTransactionTemplate()).thenReturn(new FakeTransactionTemplate());
        when(queueShard.getQueueDao()).thenReturn(queueDao);

        ReenqueueRetryStrategy strategy = mock(ReenqueueRetryStrategy.class);

        TaskExecutionResult result = TaskExecutionResult.finish();

        new TaskResultHandler(location, queueShard, strategy).handleResult(taskRecord, result);

        verify(queueShard).getTransactionTemplate();
        verify(queueDao).deleteTask(location, taskId);
        verifyZeroInteractions(strategy);
    }

    @Test
    public void should_fail_task_when_no_delay() {
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();

        TaskRecord taskRecord = TaskRecord.builder().build();
        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getTransactionTemplate()).thenReturn(new FakeTransactionTemplate());

        ReenqueueRetryStrategy strategy = mock(ReenqueueRetryStrategy.class);

        TaskExecutionResult result = TaskExecutionResult.fail();

        new TaskResultHandler(location, queueShard, strategy).handleResult(taskRecord, result);

        verifyZeroInteractions(queueShard, strategy);
    }

    @Test
    public void should_reenqueue_with_retry_strategy_task() {
        long taskId = 5L;
        QueueLocation location = QueueLocation.builder()
                .withTableName("testTable")
                .withQueueId(new QueueId("testQueue"))
                .build();

        TaskRecord taskRecord = TaskRecord.builder().withId(taskId).build();

        PostgresQueueDao queueDao = mock(PostgresQueueDao.class);

        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getTransactionTemplate()).thenReturn(new FakeTransactionTemplate());
        when(queueShard.getQueueDao()).thenReturn(queueDao);

        ReenqueueRetryStrategy strategy = mock(ReenqueueRetryStrategy.class);
        when(strategy.calculateDelay(any())).thenReturn(Duration.ofSeconds(10L));

        TaskExecutionResult result = TaskExecutionResult.reenqueue();

        new TaskResultHandler(location, queueShard, strategy).handleResult(taskRecord, result);

        verify(queueShard).getTransactionTemplate();
        verify(queueDao).reenqueue(location, taskId, Duration.ofSeconds(10L));
        verify(strategy).calculateDelay(taskRecord);
    }
}