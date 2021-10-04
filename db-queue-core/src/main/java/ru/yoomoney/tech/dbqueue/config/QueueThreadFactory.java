package ru.yoomoney.tech.dbqueue.config;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread factory for tasks execution pool.
 *
 * @author Oleg Kandaurov
 * @since 02.10.2019
 */
class QueueThreadFactory implements ThreadFactory {

    private static final Logger log = LoggerFactory.getLogger(QueueThreadFactory.class);

    private static final String THREAD_FACTORY_NAME = "queue-";
    private static final AtomicLong threadNumber = new AtomicLong(0);
    private final Thread.UncaughtExceptionHandler exceptionHandler =
            new QueueUncaughtExceptionHandler();
    @Nonnull
    private final QueueLocation location;
    @Nonnull
    private final QueueShardId shardId;

    @SuppressFBWarnings("STT_TOSTRING_STORED_IN_FIELD")
    QueueThreadFactory(@Nonnull QueueLocation location, @Nonnull QueueShardId shardId) {
        this.location = Objects.requireNonNull(location);
        this.shardId = Objects.requireNonNull(shardId);
    }

    @Override
    public Thread newThread(@Nonnull Runnable runnable) {
        String threadName = THREAD_FACTORY_NAME + threadNumber.getAndIncrement();
        Thread thread = new QueueThread(Thread.currentThread().getThreadGroup(), runnable, threadName,
                0, location, shardId);
        thread.setUncaughtExceptionHandler(exceptionHandler);
        return thread;
    }

    private static class QueueThread extends Thread {

        @Nonnull
        private final QueueLocation location;
        @Nonnull
        private final QueueShardId shardId;

        public QueueThread(ThreadGroup group, Runnable target, String name, long stackSize,
                           @Nonnull QueueLocation location, @Nonnull QueueShardId shardId) {
            super(group, target, name, stackSize);
            this.location = Objects.requireNonNull(location);
            this.shardId = Objects.requireNonNull(shardId);
        }

        @Override
        public void run() {
            log.info("starting queue thread: threadName={}, location={}, shardId={}", getName(), location, shardId);
            super.run();
            log.info("disposing queue thread: threadName={}, location={}, shardId={}", getName(), location, shardId);
        }
    }

    private static class QueueUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

        private static final Logger log = LoggerFactory.getLogger(QueueUncaughtExceptionHandler.class);

        @Override
        public void uncaughtException(Thread thread, Throwable throwable) {
            log.error("detected uncaught exception", throwable);
        }
    }
}
