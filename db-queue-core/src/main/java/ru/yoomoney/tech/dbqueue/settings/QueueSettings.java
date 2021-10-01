package ru.yoomoney.tech.dbqueue.settings;

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
 * Queue settings
 *
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public final class QueueSettings {

    private static final Function<String, Duration> DURATION_CONVERTER =
            Memoizer.memoize(Duration::parse);

    @Nonnull
    private final ProcessingSettings processingSettings;
    @Nonnull
    private final PollSettings pollSettings;
    @Nonnull
    private final FailureSettings failureSettings;
    @Nonnull
    private final ReenqueueSettings reenqueueSettings;
    @Nonnull
    private final Map<String, String> additionalSettings;

    private QueueSettings(
            @Nonnull ProcessingSettings processingSettings,
            @Nonnull PollSettings pollSettings,
            @Nonnull FailureSettings failureSettings,
            @Nonnull ReenqueueSettings reenqueueSettings,
            @Nonnull Map<String, String> additionalSettings) {
        this.processingSettings = Objects.requireNonNull(processingSettings, "processingSettings must not be null");
        this.pollSettings = Objects.requireNonNull(pollSettings, "pollSettings must not be null");
        this.failureSettings = Objects.requireNonNull(failureSettings, "failureSettings must not be null");
        this.reenqueueSettings = Objects.requireNonNull(reenqueueSettings, "reenqueueSettings must not be null");
        this.additionalSettings = Collections.unmodifiableMap(new HashMap<>(additionalSettings));
    }

    @Nonnull
    public ProcessingSettings getProcessingSettings() {
        return processingSettings;
    }

    @Nonnull
    public PollSettings getPollSettings() {
        return pollSettings;
    }

    @Nonnull
    public FailureSettings getFailureSettings() {
        return failureSettings;
    }

    /**
     * Settings for the task postponing strategy
     * when the task should be brought back to the queue.
     *
     * @return Task postponing settings.
     */
    @Nonnull
    public ReenqueueSettings getReenqueueSettings() {
        return reenqueueSettings;
    }

    /**
     * Get all additional properties for the queue.
     *
     * @return Additional queue properties.
     */
    @Nonnull
    public Map<String, String> getAdditionalSettings() {
        return additionalSettings;
    }

    /**
     * Get string value of additional queue property.
     *
     * @param settingName Name of the property.
     * @return Property value.
     */
    @Nonnull
    public String getProperty(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return Objects.requireNonNull(additionalSettings.get(settingName),
                String.format("null values are not allowed: settingName=%s", settingName));
    }

    /**
     * Get {@linkplain Duration} value of additional queue property.
     *
     * @param settingName Name of the property.
     * @return Property value.
     */
    @Nonnull
    public Duration getDurationProperty(@Nonnull String settingName) {
        return DURATION_CONVERTER.apply(getProperty(settingName));
    }

    /**
     * Create a new builder for queue settings.
     *
     * @return A new builder for queue settings.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueueSettings that = (QueueSettings) o;
        return processingSettings.equals(that.processingSettings) && pollSettings.equals(that.pollSettings) && failureSettings.equals(that.failureSettings) && reenqueueSettings.equals(that.reenqueueSettings) && additionalSettings.equals(that.additionalSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processingSettings, pollSettings, failureSettings, reenqueueSettings, additionalSettings);
    }

    @Override
    public String toString() {
        return "{" +
                "processingSettings=" + processingSettings +
                ", pollSettings=" + pollSettings +
                ", failureSettings=" + failureSettings +
                ", reenqueueSettings=" + reenqueueSettings +
                ", additionalSettings=" + additionalSettings +
                '}';
    }

    /**
     * A builder for queue settings.
     */
    public static class Builder {
        private ProcessingSettings processingSettings;
        private PollSettings pollSettings;
        private FailureSettings failureSettings;
        private ReenqueueSettings reenqueueSettings;
        private final Map<String, String> additionalSettings = new HashMap<>();

        private Builder() {
        }

        /**
         * TODO
         *
         * @param processingSettings TODO
         * @return Reference to the same builder.
         */
        public Builder withProcessingSettings(@Nonnull ProcessingSettings processingSettings) {
            this.processingSettings = processingSettings;
            return this;
        }

        /**
         * TODO
         *
         * @param pollSettings TODO
         * @return Reference to the same builder.
         */
        public Builder withPollSettings(@Nonnull PollSettings pollSettings) {
            this.pollSettings = pollSettings;
            return this;
        }

        /**
         * TODO
         *
         * @param failureSettings TODO
         * @return Reference to the same builder.
         */
        public Builder withFailureSettings(@Nonnull FailureSettings failureSettings) {
            this.failureSettings = failureSettings;
            return this;
        }

        /**
         * Set Settings for the task postponing strategy
         * when the task should be brought back to the queue.
         *
         * @param reenqueueSettings Task postponing settings.
         * @return Reference to the same builder.
         */
        public Builder withReenqueueSettings(@Nullable ReenqueueSettings reenqueueSettings) {
            this.reenqueueSettings = reenqueueSettings;
            return this;
        }

        /**
         * Set the map of additional properties for the queue.
         *
         * @param additionalSettings Additional properties for the queue.
         * @return Reference to the same builder.
         */
        public Builder withAdditionalSettings(@Nonnull Map<String, String> additionalSettings) {
            Objects.requireNonNull(additionalSettings);
            this.additionalSettings.putAll(additionalSettings);
            return this;
        }

        /**
         * Create new queue settings object.
         *
         * @return A new queue settings object.
         */
        public QueueSettings build() {
            return new QueueSettings(processingSettings, pollSettings, failureSettings, reenqueueSettings,
                    additionalSettings);
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
