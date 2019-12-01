package ru.yandex.money.common.dbqueue.internal.processing;

import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.config.QueueShard;
import ru.yandex.money.common.dbqueue.config.QueueShardId;
import ru.yandex.money.common.dbqueue.config.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.stub.FakeMillisTimeProvider;
import ru.yandex.money.common.dbqueue.stub.FakeQueueConsumer;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
@SuppressWarnings("unchecked")
public class TaskProcessorTest {

    @Test
    public void should_succesfully_process_task() {
        QueueLocation location = QueueLocation.builder().withTableName("testLocation")
                .withQueueId(new QueueId("testQueue")).build();
        TaskRecord taskRecord = TaskRecord.builder().withCreatedAt(ofSeconds(1)).withNextProcessAt(ofSeconds(5)).withPayload("testPayload").build();
        QueueShardId shardId = new QueueShardId("s1");
        String transformedPayload = "transformedPayload";
        TaskExecutionResult queueResult = TaskExecutionResult.finish();


        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getShardId()).thenReturn(shardId);
        TaskLifecycleListener listener = mock(TaskLifecycleListener.class);
        MillisTimeProvider millisTimeProvider = spy(new FakeMillisTimeProvider(Arrays.asList(3L, 5L)));
        TaskResultHandler resultHandler = mock(TaskResultHandler.class);
        TaskPayloadTransformer<String> transformer = mock(TaskPayloadTransformer.class);
        when(transformer.toObject(taskRecord.getPayload())).thenReturn(transformedPayload);
        QueueConsumer<String> queueConsumer = spy(new FakeQueueConsumer(new QueueConfig(location,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO).build()),
                transformer, r -> queueResult));


        new TaskProcessor(queueShard, listener, millisTimeProvider, resultHandler).processTask(queueConsumer, taskRecord);

        verify(listener).started(shardId, location, taskRecord);
        verify(millisTimeProvider, times(2)).getMillis();
        verify(queueConsumer).execute(Task.<String>builder(shardId)
                .withCreatedAt(taskRecord.getCreatedAt())
                .withPayload(transformedPayload)
                .withAttemptsCount(taskRecord.getAttemptsCount())
                .withReenqueueAttemptsCount(taskRecord.getReenqueueAttemptsCount())
                .withTotalAttemptsCount(taskRecord.getTotalAttemptsCount())
                .withExtData(Collections.emptyMap()).build());
        verify(listener).executed(shardId, location, taskRecord, queueResult, 2);
        verify(resultHandler).handleResult(taskRecord, queueResult);
        verify(listener).finished(shardId, location, taskRecord);

    }

    @Test
    public void should_handle_exception_when_queue_failed() {
        QueueLocation location = QueueLocation.builder().withTableName("testLocation")
                .withQueueId(new QueueId("testQueue")).build();
        TaskRecord taskRecord = TaskRecord.builder().withCreatedAt(ofSeconds(1)).withNextProcessAt(ofSeconds(5)).withPayload("testPayload").build();
        QueueShardId shardId = new QueueShardId("s1");
        RuntimeException queueException = new RuntimeException("fail");


        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getShardId()).thenReturn(shardId);
        TaskLifecycleListener listener = mock(TaskLifecycleListener.class);
        MillisTimeProvider millisTimeProvider = mock(MillisTimeProvider.class);
        TaskResultHandler resultHandler = mock(TaskResultHandler.class);
        TaskPayloadTransformer<String> transformer = mock(TaskPayloadTransformer.class);
        when(transformer.toObject(taskRecord.getPayload())).thenReturn(taskRecord.getPayload());
        QueueConsumer<String> queueConsumer = spy(new FakeQueueConsumer(new QueueConfig(location,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO).build()),
                transformer, r -> {
            throw queueException;
        }));


        new TaskProcessor(queueShard, listener, millisTimeProvider, resultHandler).processTask(queueConsumer, taskRecord);

        verify(listener).started(shardId, location, taskRecord);
        verify(queueConsumer).execute(any());
        verify(listener).crashed(shardId, location, taskRecord, queueException);
        verify(listener).finished(shardId, location, taskRecord);

    }

    @Test
    public void should_handle_exception_when_result_handler_failed() {
        QueueLocation location = QueueLocation.builder().withTableName("testLocation")
                .withQueueId(new QueueId("testQueue")).build();
        TaskRecord taskRecord = TaskRecord.builder().withCreatedAt(ofSeconds(1)).withNextProcessAt(ofSeconds(5)).withPayload("testPayload").build();
        QueueShardId shardId = new QueueShardId("s1");
        RuntimeException handlerException = new RuntimeException("fail");
        TaskExecutionResult queueResult = TaskExecutionResult.finish();


        QueueShard queueShard = mock(QueueShard.class);
        when(queueShard.getShardId()).thenReturn(shardId);
        TaskLifecycleListener listener = mock(TaskLifecycleListener.class);
        MillisTimeProvider millisTimeProvider = mock(MillisTimeProvider.class);
        TaskResultHandler resultHandler = mock(TaskResultHandler.class);
        doThrow(handlerException).when(resultHandler).handleResult(any(), any());
        TaskPayloadTransformer<String> transformer = mock(TaskPayloadTransformer.class);
        when(transformer.toObject(taskRecord.getPayload())).thenReturn(taskRecord.getPayload());
        QueueConsumer<String> queueConsumer = spy(new FakeQueueConsumer(new QueueConfig(location,
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO).build()),
                transformer, r -> queueResult));


        new TaskProcessor(queueShard, listener, millisTimeProvider, resultHandler).processTask(queueConsumer, taskRecord);

        verify(listener).started(shardId, location, taskRecord);
        verify(queueConsumer).execute(any());
        verify(resultHandler).handleResult(taskRecord, queueResult);
        verify(listener).crashed(shardId, location, taskRecord, handlerException);
        verify(listener).finished(shardId, location, taskRecord);
    }


    private ZonedDateTime ofSeconds(int seconds) {
        return ZonedDateTime.of(0, 1, 1, 0, 0, seconds, 0, ZoneId.systemDefault());
    }

}