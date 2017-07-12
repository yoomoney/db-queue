package ru.yandex.money.common.dbqueue.internal;

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
     * Приостановить исполнение кода
     *
     * @param timeout промежуток на который следует приостановить работу
     */
    void doWait(Duration timeout);

    /**
     * Cтратегия выполнения задачи в потоке
     */
    class ThreadLoopPolicy implements LoopPolicy {

        private static final Logger log = LoggerFactory.getLogger(ThreadLoopPolicy.class);

        @Override
        public void doRun(Runnable runnable) {
            while (!Thread.currentThread().isInterrupted()) {
                runnable.run();
            }
        }

        @Override
        public void doWait(Duration timeout) {
            try {
                Thread.sleep(timeout.toMillis());
            } catch (InterruptedException ignored) {
                log.info("sleep interrupted: threadName={}", Thread.currentThread().getName());
                Thread.currentThread().interrupt();
            }
        }

    }
}
