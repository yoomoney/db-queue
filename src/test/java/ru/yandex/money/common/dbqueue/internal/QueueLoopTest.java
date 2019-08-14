package ru.yandex.money.common.dbqueue.internal;

import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.ThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.internal.runner.QueueRunner;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.stub.FakeMillisTimeProvider;

import java.time.Duration;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class QueueLoopTest {

    @Test
    public void should_perform_success_lifecycle() throws Exception {
        LoopPolicy loopPolicy = spy(new SyncLoopPolicy());
        ThreadLifecycleListener listener = mock(ThreadLifecycleListener.class);
        QueueShardId shardId = new QueueShardId("s1");
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        QueueLocation location = QueueLocation.builder().withTableName("table")
                .withQueueId(new QueueId("queue")).build();
        Duration waitDuration = Duration.ofMillis(100L);
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location,
                QueueSettings.builder()
                        .withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(waitDuration)
                        .build()));
        QueueRunner queueRunner = mock(QueueRunner.class);
        when(queueRunner.runQueue(queueConsumer)).thenReturn(QueueProcessingStatus.SKIPPED);

        FakeMillisTimeProvider millisTimeProvider = new FakeMillisTimeProvider(7, 11);

        new QueueLoop(loopPolicy, listener, millisTimeProvider).start(shardId, queueConsumer, queueRunner);

        verify(loopPolicy).doRun(any());
        verify(listener).started(shardId, location);
        verify(queueRunner).runQueue(queueConsumer);
        verify(listener).executed(shardId, location, false, 4);
        verify(loopPolicy).doWait(waitDuration, LoopPolicy.WaitInterrupt.ALLOW);
        verify(listener).finished(shardId, location);
    }

    @Test
    public void should_wakeup() {
        LoopPolicy loopPolicy = spy(new DelegatedSingleLoopExecution(new LoopPolicy.WakeupLoopPolicy()));
        ThreadLifecycleListener listener = mock(ThreadLifecycleListener.class);
        QueueShardId shardId = new QueueShardId("s1");
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        QueueLocation location = QueueLocation.builder().withTableName("table")
                .withQueueId(new QueueId("queue")).build();
        Duration waitDuration = Duration.ofDays(1);
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location,
                QueueSettings.builder()
                        .withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(waitDuration)
                        .build()));
        QueueRunner queueRunner = mock(QueueRunner.class);
        when(queueRunner.runQueue(queueConsumer)).thenReturn(QueueProcessingStatus.SKIPPED);


        QueueLoop queueLoop = new QueueLoop(loopPolicy, listener, new MillisTimeProvider.SystemMillisTimeProvider());
        queueLoop.wakeup();
        queueLoop.start(shardId, queueConsumer, queueRunner);

        verify(loopPolicy).doRun(any());
        verify(listener).started(shardId, location);
        verify(queueRunner).runQueue(queueConsumer);
        verify(listener).executed(eq(shardId), eq(location), eq(false), anyLong());
        verify(loopPolicy).doWait(waitDuration, LoopPolicy.WaitInterrupt.ALLOW);
        verify(listener).finished(shardId, location);

        queueLoop.terminate();
        verify(loopPolicy).doTerminate();
    }

    @Test
    public void should_perform_crash_lifecycle() throws Exception {
        LoopPolicy loopPolicy = spy(new SyncLoopPolicy());
        ThreadLifecycleListener listener = mock(ThreadLifecycleListener.class);
        QueueShardId shardId = new QueueShardId("s1");
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        QueueLocation location = QueueLocation.builder().withTableName("table")
                .withQueueId(new QueueId("queue")).build();
        Duration fatalCrashTimeout = Duration.ofDays(1L);
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location,
                QueueSettings.builder()
                        .withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO)
                        .withFatalCrashTimeout(fatalCrashTimeout)
                        .build()));
        QueueRunner queueRunner = mock(QueueRunner.class);
        RuntimeException exception = new RuntimeException("exc");
        when(queueRunner.runQueue(queueConsumer)).thenThrow(exception);

        new QueueLoop(loopPolicy, listener, mock(MillisTimeProvider.class)).start(shardId, queueConsumer, queueRunner);

        verify(loopPolicy).doRun(any());
        verify(listener).started(shardId, location);
        verify(queueRunner).runQueue(queueConsumer);
        verify(loopPolicy).doWait(fatalCrashTimeout, LoopPolicy.WaitInterrupt.DENY);
        verify(listener).crashed(shardId, location, exception);
        verify(listener).finished(shardId, location);
    }

    private static class DelegatedSingleLoopExecution implements LoopPolicy {

        private final LoopPolicy delegate;
        private int attemptCount = 0;

        private DelegatedSingleLoopExecution(LoopPolicy delegate) {
            this.delegate = delegate;
        }

        @Override
        public void doRun(Runnable runnable) {
            delegate.doRun(() -> {
                if (attemptCount > 0) {
                    Thread.currentThread().interrupt();
                    return;
                }
                attemptCount++;
                runnable.run();
            });
        }

        @Override
        public void doContinue() {
            delegate.doContinue();
        }

        @Override
        public void doWait(Duration timeout, WaitInterrupt waitInterrupt) {
            delegate.doWait(timeout, waitInterrupt);
        }

        @Override
        public void doTerminate() {
            delegate.doTerminate();
        }
    }

    private static class SyncLoopPolicy implements LoopPolicy {
        @Override
        public void doRun(Runnable runnable) {
            runnable.run();
        }

        @Override
        public void doContinue() {

        }

        @Override
        public void doWait(Duration timeout, WaitInterrupt waitInterrupt) {

        }

        @Override
        public void doTerminate() {

        }
    }
}