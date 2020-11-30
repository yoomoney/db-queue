package ru.yoomoney.tech.dbqueue.internal.processing;

import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.ThreadLifecycleListener;
import ru.yoomoney.tech.dbqueue.internal.runner.QueueRunner;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.stub.FakeMillisTimeProvider;

import java.time.Duration;
import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
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

        FakeMillisTimeProvider millisTimeProvider = new FakeMillisTimeProvider(Arrays.asList(7L, 11L));

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
        queueLoop.unpause();
        queueLoop.wakeup();
        queueLoop.start(shardId, queueConsumer, queueRunner);

        verify(loopPolicy).doRun(any());
        verify(listener).started(shardId, location);
        verify(queueRunner).runQueue(queueConsumer);
        verify(listener).executed(eq(shardId), eq(location), eq(false), anyLong());
        verify(loopPolicy).doWait(waitDuration, LoopPolicy.WaitInterrupt.ALLOW);
        verify(listener).finished(shardId, location);

        queueLoop.pause();
        verify(loopPolicy).pause();
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
        public void pause() {
            delegate.pause();
        }

        @Override
        public void unpause() {
            delegate.unpause();
        }

        @Override
        public boolean isPaused() {
            return delegate.isPaused();
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
        public boolean isPaused() {
            return false;
        }

        @Override
        public void pause() {

        }

        @Override
        public void unpause() {

        }
    }
}