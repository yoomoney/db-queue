package ru.yoomoney.tech.dbqueue.internal.processing;

import java.time.Duration;

public class DelegatedSingleQueueLoopExecution implements QueueLoop {

    private final QueueLoop delegate;
    private int attemptCount = 0;

    DelegatedSingleQueueLoopExecution(QueueLoop delegate) {
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
