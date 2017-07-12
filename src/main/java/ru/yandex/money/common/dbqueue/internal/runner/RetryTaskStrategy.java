package ru.yandex.money.common.dbqueue.internal.runner;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.settings.TaskRetryType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
     * Получить placeholders для подстановки в sql со временем обработки
     *
     * @return параметры подстановки sql
     */
    @Nullable
    MapSqlParameterSource getPlaceholders();

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
                    return new GeometricBackoff();
                case ARITHMETIC_BACKOFF:
                    return new ArithmeticBackoff();
                case FIXED_INTERVAL:
                    return new FixedInterval(settings);
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

        @Nonnull
        @Override
        public String getNextProcessTimeSql() {
            return "now() + power(2, attempt) * 60 * INTERVAL '1 SECOND'";
        }

        @Nullable
        @Override
        public MapSqlParameterSource getPlaceholders() {
            return null;
        }
    }

    /**
     * Реализация стратегии откладывания для
     * {@link TaskRetryType#ARITHMETIC_BACKOFF}
     */
    class ArithmeticBackoff implements RetryTaskStrategy {

        @Nonnull
        @Override
        public String getNextProcessTimeSql() {
            return "now() + (1 + (attempt * 2)) * 60 * INTERVAL '1 SECOND'";
        }

        @Nullable
        @Override
        public MapSqlParameterSource getPlaceholders() {
            return null;
        }

    }

    /**
     * Реализация стратегии откладывания для
     * {@link TaskRetryType#FIXED_INTERVAL}
     */
    class FixedInterval implements RetryTaskStrategy {

        private final MapSqlParameterSource placeholders;

        /**
         * Конструктор стратегии с фиксированным интервалом откладывания
         *
         * @param executionSettings настройки задачи. Требует наличия настройки
         *                          {@link QueueSettings.AdditionalSetting#RETRY_FIXED_INTERVAL_DELAY}
         */
        FixedInterval(@Nonnull QueueSettings executionSettings) {
            Objects.requireNonNull(executionSettings);
            Duration fixedDelay = executionSettings.getDurationProperty(
                    QueueSettings.AdditionalSetting.RETRY_FIXED_INTERVAL_DELAY);
            this.placeholders = new MapSqlParameterSource().addValue("delay", fixedDelay.getSeconds());
        }

        @Nonnull
        @Override
        public String getNextProcessTimeSql() {
            return "now() + :delay * INTERVAL '1 SECOND'";
        }

        @Nullable
        @Override
        public MapSqlParameterSource getPlaceholders() {
            return placeholders;
        }


    }


}
