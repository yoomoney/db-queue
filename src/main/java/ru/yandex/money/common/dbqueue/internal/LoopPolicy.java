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
     * @param timeout       промежуток на который следует приостановить работу
     * @param waitInterrupt признак, что разрешено прервать ожидание и продолжить работу
     */
    void doWait(Duration timeout, WaitInterrupt waitInterrupt);

    /**
     * Прервать выполнение кода
     */
    void doTerminate();

    /**
     * Cтратегия выполнения задачи в потоке
     */
    @SuppressFBWarnings("LO_SUSPECT_LOG_CLASS")
    class WakeupLoopPolicy implements LoopPolicy {
        private static final Logger log = LoggerFactory.getLogger(LoopPolicy.class);

        private final Object monitor = new Object();
        private volatile boolean isWakedUp = false;
        private volatile boolean terminated;

        @Override
        public void doRun(Runnable runnable) {
            terminated = false;
            while (!terminated && !Thread.currentThread().isInterrupted()) {
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
        public void doWait(Duration timeout, WaitInterrupt waitInterrupt) {
            try {
                synchronized (monitor) {
                    long plannedWakeupTime = System.currentTimeMillis() + timeout.toMillis();
                    long timeToSleep = plannedWakeupTime - System.currentTimeMillis();
                    while (timeToSleep > 1L) {
                        if (!isWakedUp) {
                            monitor.wait(timeToSleep);
                        }
                        if (isWakedUp && waitInterrupt == WaitInterrupt.ALLOW) {
                            break;
                        }
                        if (isWakedUp && waitInterrupt == WaitInterrupt.DENY) {
                            isWakedUp = false;
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

        @Override
        public void doTerminate() {
            terminated = true;
            log.debug("doTerminate(): threadName={}", Thread.currentThread().getName());
        }
    }

    /**
     * Признак прерывания ожидания
     */
    enum WaitInterrupt {
        /**
         * Прерывание разрешено
         */
        ALLOW,
        /**
         * Прерывание запрещено
         */
        DENY
    }

}
