package ru.yandex.money.common.dbqueue.settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Настройки очереди
 *
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public final class QueueSettings {

    private static final Function<String, Duration> DURATION_CONVERTER =
            Memoizer.memoize(Duration::parse);

    private static final Duration DEFAULT_TIMEOUT_ON_FATAL_CRASH = Duration.ofSeconds(1L);

    private final int threadCount;
    @Nonnull
    private final Duration noTaskTimeout;
    @Nonnull
    private final Duration betweenTaskTimeout;
    @Nonnull
    private final Duration fatalCrashTimeout;
    @Nonnull
    private final Duration retryInterval;
    @Nonnull
    private final TaskRetryType retryType;
    @Nonnull
    private final ReenqueueRetrySettings reenqueueRetrySettings;
    @Nonnull
    private final ProcessingMode processingMode;
    @Nonnull
    private final Map<String, String> additionalSettings;

    private QueueSettings(@Nonnull Duration noTaskTimeout,
                          @Nonnull Duration betweenTaskTimeout,
                          @Nullable Duration fatalCrashTimeout,
                          @Nullable Integer threadCount,
                          @Nullable TaskRetryType retryType,
                          @Nullable Duration retryInterval,
                          @Nullable ReenqueueRetrySettings reenqueueRetrySettings,
                          @Nullable ProcessingMode processingMode,
                          @Nullable Map<String, String> additionalSettings) {
        this.noTaskTimeout = Objects.requireNonNull(noTaskTimeout);
        this.betweenTaskTimeout = Objects.requireNonNull(betweenTaskTimeout);
        this.threadCount = threadCount == null ? 1 : threadCount;
        this.fatalCrashTimeout = fatalCrashTimeout == null ? DEFAULT_TIMEOUT_ON_FATAL_CRASH : fatalCrashTimeout;
        this.retryType = retryType == null ? TaskRetryType.GEOMETRIC_BACKOFF : retryType;
        this.retryInterval = retryInterval == null ? Duration.ofMinutes(1) : retryInterval;
        this.reenqueueRetrySettings = reenqueueRetrySettings == null
                ? ReenqueueRetrySettings.createDefault()
                : reenqueueRetrySettings;
        this.processingMode = processingMode == null ? ProcessingMode.SEPARATE_TRANSACTIONS : processingMode;
        this.additionalSettings = additionalSettings == null ? Collections.emptyMap() :
                Collections.unmodifiableMap(new HashMap<>(additionalSettings));
    }

    /**
     * Получить стратегию повтора задачи
     *
     * @return стратегия повтора задачи
     */
    @Nonnull
    public TaskRetryType getRetryType() {
        return retryType;
    }

    /**
     * Получить базовый интервал повтора задачи
     *
     * @return интервал повтора задачи
     * @see TaskRetryType
     */
    @Nonnull
    public Duration getRetryInterval() {
        return retryInterval;
    }

    /**
     * Настройки стратегии переоткладывания задач в случае, если задачу требуется вернуть в очередь.
     *
     * @return настройки
     */
    @Nonnull
    public ReenqueueRetrySettings getReenqueueRetrySettings() {
        return reenqueueRetrySettings;
    }

    /**
     * Получить количество потоков обработки очереди.
     *
     * @return количество потоков
     */
    public int getThreadCount() {
        return threadCount;
    }

    /**
     * Получить задержку обработки при отсутствии задач на обработку
     *
     * @return таймаут при отсутствии задач
     */
    @Nonnull
    public Duration getNoTaskTimeout() {
        return noTaskTimeout;
    }

    /**
     * Получить задержку выполенения после обработки очередной задачи
     *
     * @return таймаут после обработки очередной задачи
     */
    @Nonnull
    public Duration getBetweenTaskTimeout() {
        return betweenTaskTimeout;
    }

    /**
     * Получить таймаут на который поток очереди засыпает после непредвиденной ошибки
     *
     * @return таймаут после непредиденной ошибки
     */
    @Nonnull
    public Duration getFatalCrashTimeout() {
        return fatalCrashTimeout;
    }

    /**
     * Получить режим обработки очереди
     *
     * @return режим обработки очереди
     */
    @Nonnull
    public ProcessingMode getProcessingMode() {
        return processingMode;
    }

    /**
     * Получить все дополнительные настройки очереди.
     *
     * @return дополнительные настройки очереди
     */
    @Nonnull
    public Map<String, String> getAdditionalSettings() {
        return additionalSettings;
    }

    /**
     * Получить строковое значение дополнительной настройки
     *
     * @param settingName имя настройки
     * @return значение настройки
     */
    @Nonnull
    public String getProperty(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return Objects.requireNonNull(additionalSettings.get(settingName),
                String.format("null values are not allowed: settingName=%s", settingName));
    }

    /**
     * Получить значение настройки в формате Duration
     *
     * @param settingName имя настройки
     * @return значение настройки
     */
    @Nonnull
    public Duration getDurationProperty(@Nonnull String settingName) {
        return DURATION_CONVERTER.apply(getProperty(settingName));
    }

    /**
     * Создать билдер настроек
     *
     * @return билдер настроек
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return '{' +
                "threadCount=" + threadCount +
                ", betweenTaskTimeout=" + betweenTaskTimeout +
                ", noTaskTimeout=" + noTaskTimeout +
                ", processingMode=" + processingMode +
                ", retryType=" + retryType +
                ", retryInterval=" + retryInterval +
                ", reenqueueRetrySettings=" + reenqueueRetrySettings +
                ", fatalCrashTimeout=" + fatalCrashTimeout +
                (additionalSettings.isEmpty() ? "" : ", additionalSettings=" + additionalSettings) +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        QueueSettings that = (QueueSettings) obj;
        return threadCount == that.threadCount &&
                retryType == that.retryType &&
                processingMode == that.processingMode &&
                Objects.equals(reenqueueRetrySettings, that.reenqueueRetrySettings) &&
                Objects.equals(noTaskTimeout, that.noTaskTimeout) &&
                Objects.equals(betweenTaskTimeout, that.betweenTaskTimeout) &&
                Objects.equals(fatalCrashTimeout, that.fatalCrashTimeout) &&
                Objects.equals(retryInterval, that.retryInterval) &&
                Objects.equals(additionalSettings, that.additionalSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(threadCount, noTaskTimeout, betweenTaskTimeout, fatalCrashTimeout, retryType, reenqueueRetrySettings,
                processingMode, retryInterval, additionalSettings);
    }

    /**
     * Билдер настроек очереди
     */
    public static class Builder {
        private Duration noTaskTimeout;
        private Duration betweenTaskTimeout;
        private Duration fatalCrashTimeout;
        private Integer threadCount;
        private TaskRetryType retryType;
        private Duration retryInterval;
        private ReenqueueRetrySettings reenqueueRetrySettings;
        private ProcessingMode processingMode;
        private final Map<String, String> additionalSettings = new HashMap<>();

        private Builder() {
        }

        /**
         * Установить задержку обработки в случае отсутствия задач в очереди
         *
         * @param noTaskTimeout таймаут задержки
         * @return билдер настроек очереди
         */
        public Builder withNoTaskTimeout(@Nonnull Duration noTaskTimeout) {
            this.noTaskTimeout = Objects.requireNonNull(noTaskTimeout);
            return this;
        }

        /**
         * Установить задержку между обработкой задач в очередиы
         *
         * @param betweenTaskTimeout таймаут задержки
         * @return билдер настроек очереди
         */
        public Builder withBetweenTaskTimeout(@Nonnull Duration betweenTaskTimeout) {
            this.betweenTaskTimeout = Objects.requireNonNull(betweenTaskTimeout);
            return this;
        }

        /**
         * Установить задержку обработки после непредвиденной ошибки при обработке очереди
         *
         * @param fatalCrashTimeout таймаут после непридвиденной ошибки
         * @return билдер настроек очереди
         */
        public Builder withFatalCrashTimeout(@Nullable Duration fatalCrashTimeout) {
            this.fatalCrashTimeout = fatalCrashTimeout;
            return this;
        }

        /**
         * Установить количество потоков обработки очереди
         *
         * @param threadCount количество потоков
         * @return билдер настроек очереди
         */
        public Builder withThreadCount(@Nullable Integer threadCount) {
            this.threadCount = threadCount;
            return this;
        }

        /**
         * Установить стратегию повтора неуспешной задачи
         *
         * @param taskRetryType стратегия повтора
         * @return билдер настроек очереди
         */
        public Builder withRetryType(@Nullable TaskRetryType taskRetryType) {
            this.retryType = taskRetryType;
            return this;
        }

        /**
         * Установить базовый интервал повтора задачи
         *
         * @param retryInterval интервал повтора
         * @return билдер настроек очереди
         */
        public Builder withRetryInterval(@Nullable Duration retryInterval) {
            this.retryInterval = retryInterval;
            return this;
        }

        /**
         * Установить настройки стратегии переоткладывания задач в случае, если задачу требуется вернуть в очередь.
         *
         * @param reenqueueRetrySettings настройки
         * @return билдер настроек очереди
         */
        public Builder withReenqueueRetrySettings(@Nullable ReenqueueRetrySettings reenqueueRetrySettings) {
            this.reenqueueRetrySettings = reenqueueRetrySettings;
            return this;
        }

        /**
         * Режим обработки задач в очереди
         *
         * @param processingMode режим обработки
         * @return билдер настроек очереди
         */
        public Builder withProcessingMode(@Nullable ProcessingMode processingMode) {
            this.processingMode = processingMode;
            return this;
        }

        /**
         * Установить набор дополнительных настроек.
         *
         * @param additionalSettings дополнительные настройки
         * @return билдер настроек очереди
         */
        public Builder withAdditionalSettings(@Nonnull Map<String, String> additionalSettings) {
            Objects.requireNonNull(additionalSettings);
            this.additionalSettings.putAll(additionalSettings);
            return this;
        }

        /**
         * Установить значение дополнительной настройку
         *
         * @param settingName имя настройки
         * @param value       значение настройки
         * @return билдер настроек очереди
         */
        public Builder putSetting(String settingName, String value) {
            additionalSettings.put(settingName, value);
            return this;
        }

        /**
         * Сконструировать объект настроек.
         *
         * @return объект настроек
         */
        public QueueSettings build() {
            return new QueueSettings(noTaskTimeout, betweenTaskTimeout, fatalCrashTimeout, threadCount,
                    retryType, retryInterval, reenqueueRetrySettings, processingMode, additionalSettings);
        }
    }


    private static class Memoizer<T, U> {

        private final Map<T, U> cache = new ConcurrentHashMap<>();

        private Function<T, U> doMemoize(Function<T, U> function) {
            return input -> cache.computeIfAbsent(input, function);
        }

        private static <T, U> Function<T, U> memoize(Function<T, U> function) {
            return new Memoizer<T, U>().doMemoize(function);
        }
    }
}
