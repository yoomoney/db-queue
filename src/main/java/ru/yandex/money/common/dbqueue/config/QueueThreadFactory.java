package ru.yandex.money.common.dbqueue.config;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread factory for tasks execution pool.
 *
 * @author Oleg Kandaurov
 * @since 02.10.2019
 */
class QueueThreadFactory implements ThreadFactory {

    private static final Logger log = LoggerFactory.getLogger(QueueThreadFactory.class);

    private static final String THREAD_FACTORY_NAME = "queue-";
    private static final AtomicInteger threadNumber = new AtomicInteger(0);
    private final ThreadFactory backingThreadFactory = Executors.defaultThreadFactory();
    private final Thread.UncaughtExceptionHandler exceptionHandler =
            new QueueUncaughtExceptionHandler();
    private final QueueLocation location;
    private final QueueShardId shardId;

    @SuppressFBWarnings("STT_TOSTRING_STORED_IN_FIELD")
    QueueThreadFactory(@Nonnull QueueLocation location, @Nonnull QueueShardId shardId) {
        this.location = Objects.requireNonNull(location);
        this.shardId = Objects.requireNonNull(shardId);
    }

    @Override
    public Thread newThread(@Nonnull Runnable runnable) {
        Thread thread = backingThreadFactory.newThread(runnable);
        String threadName = THREAD_FACTORY_NAME + threadNumber.getAndIncrement();
        thread.setName(threadName);
        thread.setUncaughtExceptionHandler(exceptionHandler);
        log.info("created queue thread: threadName={}, location={}, shardId={}", threadName, location, shardId);
        return thread;
    }

    private static class QueueUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

        private static final Logger log = LoggerFactory.getLogger(QueueUncaughtExceptionHandler.class);

        @Override
        public void uncaughtException(Thread thread, Throwable throwable) {
            log.error("detected uncaught exception", throwable);
        }
    }
}
