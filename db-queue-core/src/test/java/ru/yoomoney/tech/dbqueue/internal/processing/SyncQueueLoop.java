package ru.yoomoney.tech.dbqueue.internal.processing;

import java.time.Duration;

public class SyncQueueLoop implements QueueLoop {
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
