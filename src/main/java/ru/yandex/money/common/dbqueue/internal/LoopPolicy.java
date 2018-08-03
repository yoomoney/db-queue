package ru.yandex.money.common.dbqueue.internal;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Вспомогательный класс, для задания стратегии
 * выполнения задачи в цикле
 *
 * @author Oleg Kandaurov
 * @since 15.07.2017
 */
public interface LoopPolicy {

    /**
     * Запустить выполнение кода
     *
     * @param runnable код для исполнения
     */
    void doRun(Runnable runnable);

    /**
     * Продолжить исполнение кода
     */
    void doContinue();

    /**
     * Приостановить исполнение кода
     *
     * @param timeout промежуток на который следует приостановить работу
     */
    void doWait(Duration timeout);

    /**
     * Cтратегия выполнения задачи в потоке
     */
    @SuppressFBWarnings("LO_SUSPECT_LOG_CLASS")
    class WakeupLoopPolicy implements LoopPolicy {
        private static final Logger log = LoggerFactory.getLogger(LoopPolicy.class);

        private final Object monitor = new Object();
        private volatile boolean isWakedUp = false;

        @Override
        public void doRun(Runnable runnable) {
            while (!Thread.currentThread().isInterrupted()) {
                runnable.run();
            }

        }

        @Override
        public void doContinue() {
            synchronized (monitor) {
                isWakedUp = true;
                monitor.notify();
            }
        }

        @Override
        public void doWait(Duration timeout) {
            try {
                synchronized (monitor) {
                    long plannedWakeupTime = System.currentTimeMillis() + timeout.toMillis();
                    long timeToSleep = plannedWakeupTime - System.currentTimeMillis();
                    while (timeToSleep > 1L) {
                        if (!isWakedUp) {
                            monitor.wait(timeToSleep);
                        }
                        if (isWakedUp) {
                            break;
                        }
                        timeToSleep = plannedWakeupTime - System.currentTimeMillis();
                    }
                    isWakedUp = false;
                }
            } catch (InterruptedException ignored) {
                log.info("sleep interrupted: threadName={}", Thread.currentThread().getName());
                Thread.currentThread().interrupt();
            }

        }
    }

}
