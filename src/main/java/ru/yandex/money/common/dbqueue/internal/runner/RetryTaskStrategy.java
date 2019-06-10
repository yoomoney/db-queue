package ru.yandex.money.common.dbqueue.internal.runner;

import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.settings.TaskRetryType;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Objects;

/**
 * Интерфейс стратегии откладывания задачи при повторной обработке
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
interface RetryTaskStrategy {

    /**
     * Получить sql, в котором содержится инструкция обновления времени обработки
     *
     * @return sql cо временем обработки
     */
    @Nonnull
    String getNextProcessTimeSql();

    /**
     * Получить базовый интервал повтора
     * @return интервал повтора
     */
    @Nonnull
    Duration getBaseRetryInterval();

    /**
     * Фабрика для создания стратегий откладывания задач при повторе
     */
    final class Factory {

        private Factory() {
        }

        /**
         * Создать стратегию откладывания задачи
         *
         * @param settings настройки очереди
         * @return стратегия откладывания задачи
         */
        @Nonnull
        static RetryTaskStrategy create(@Nonnull QueueSettings settings) {
            Objects.requireNonNull(settings);
            switch (settings.getRetryType()) {
                case GEOMETRIC_BACKOFF:
                    return new GeometricBackoff(settings);
                case ARITHMETIC_BACKOFF:
                    return new ArithmeticBackoff(settings);
                case LINEAR_BACKOFF:
                    return new LinearBackoff(settings);
                default:
                    throw new IllegalStateException("unknown growth type: " + settings.getRetryType());
            }
        }
    }

    /**
     * Реализация стратегии откладывания для
     * {@link TaskRetryType#GEOMETRIC_BACKOFF}
     */
    class GeometricBackoff implements RetryTaskStrategy {

        private final QueueSettings queueSettings;

        /**
         * Конструктор
         *
         * @param queueSettings настройки очереди
         */
        GeometricBackoff(@Nonnull QueueSettings queueSettings) {
            this.queueSettings = Objects.requireNonNull(queueSettings);
        }

        @Nonnull
        @Override
        public String getNextProcessTimeSql() {
            return "now() + power(2, attempt) * :retryInterval :: interval";
        }

        @Nonnull
        @Override
        public Duration getBaseRetryInterval() {
            return queueSettings.getRetryInterval();
        }
    }

    /**
     * Реализация стратегии откладывания для
     * {@link TaskRetryType#ARITHMETIC_BACKOFF}
     */
    class ArithmeticBackoff implements RetryTaskStrategy {

        private final QueueSettings queueSettings;

        /**
         * Конструктор
         *
         * @param queueSettings настройки очереди
         */
        ArithmeticBackoff(@Nonnull QueueSettings queueSettings) {
            this.queueSettings = Objects.requireNonNull(queueSettings);
        }

        @Nonnull
        @Override
        public String getNextProcessTimeSql() {
            return "now() + (1 + (attempt * 2)) * :retryInterval :: interval";
        }

        @Nonnull
        @Override
        public Duration getBaseRetryInterval() {
            return queueSettings.getRetryInterval();
        }
    }

    /**
     * Реализация стратегии откладывания для
     * {@link TaskRetryType#LINEAR_BACKOFF}
     */
    class LinearBackoff implements RetryTaskStrategy {

        private final QueueSettings queueSettings;

        /**
         * Конструктор стратегии с фиксированным интервалом откладывания
         *
         * @param queueSettings настройки очереди.
         */
        LinearBackoff(@Nonnull QueueSettings queueSettings) {
            this.queueSettings = Objects.requireNonNull(queueSettings);
        }

        @Nonnull
        @Override
        public String getNextProcessTimeSql() {
            return "now() + :retryInterval :: interval";
        }

        @Nonnull
        @Override
        public Duration getBaseRetryInterval() {
            return queueSettings.getRetryInterval();
        }
    }

}
