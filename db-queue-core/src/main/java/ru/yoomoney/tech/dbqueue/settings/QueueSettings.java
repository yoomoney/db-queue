package ru.yoomoney.tech.dbqueue.settings;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Queue settings
 *
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public final class QueueSettings {

    @Nonnull
    private final ProcessingSettings processingSettings;
    @Nonnull
    private final PollSettings pollSettings;
    @Nonnull
    private final FailureSettings failureSettings;
    @Nonnull
    private final ReenqueueSettings reenqueueSettings;
    @Nonnull
    private final ExtSettings extSettings;

    private QueueSettings(
            @Nonnull ProcessingSettings processingSettings,
            @Nonnull PollSettings pollSettings,
            @Nonnull FailureSettings failureSettings,
            @Nonnull ReenqueueSettings reenqueueSettings,
            @Nonnull ExtSettings extSettings) {
        this.processingSettings = Objects.requireNonNull(processingSettings, "processingSettings must not be null");
        this.pollSettings = Objects.requireNonNull(pollSettings, "pollSettings must not be null");
        this.failureSettings = Objects.requireNonNull(failureSettings, "failureSettings must not be null");
        this.reenqueueSettings = Objects.requireNonNull(reenqueueSettings, "reenqueueSettings must not be null");
        this.extSettings = Objects.requireNonNull(extSettings, "extSettings must not be null");
    }

    /**
     * Get task processing settings.
     *
     * @return polling settings.
     */
    @Nonnull
    public ProcessingSettings getProcessingSettings() {
        return processingSettings;
    }

    /**
     * Get task polling settings.
     *
     * @return polling settings.
     */
    @Nonnull
    public PollSettings getPollSettings() {
        return pollSettings;
    }

    /**
     * Settings for task execution strategy in case of failure.
     *
     * @return failure settings.
     */
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
    public ExtSettings getExtSettings() {
        return extSettings;
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
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        QueueSettings that = (QueueSettings) obj;
        return processingSettings.equals(that.processingSettings) && pollSettings.equals(that.pollSettings) &&
                failureSettings.equals(that.failureSettings) && reenqueueSettings.equals(that.reenqueueSettings)
                && extSettings.equals(that.extSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processingSettings, pollSettings, failureSettings, reenqueueSettings, extSettings);
    }

    @Override
    public String toString() {
        return "{" +
                "processingSettings=" + processingSettings +
                ", pollSettings=" + pollSettings +
                ", failureSettings=" + failureSettings +
                ", reenqueueSettings=" + reenqueueSettings +
                ", additionalSettings=" + extSettings +
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
        private ExtSettings extSettings;

        private Builder() {
        }

        /**
         * Sets task processing settings.
         *
         * @param processingSettings processing settings
         * @return Reference to the same builder.
         */
        public Builder withProcessingSettings(@Nonnull ProcessingSettings processingSettings) {
            this.processingSettings = processingSettings;
            return this;
        }

        /**
         * Sets task polling settings
         *
         * @param pollSettings poll settings
         * @return Reference to the same builder.
         */
        public Builder withPollSettings(@Nonnull PollSettings pollSettings) {
            this.pollSettings = pollSettings;
            return this;
        }

        /**
         * Sets settings for task execution strategy in case of failure.
         *
         * @param failureSettings fail postpone settings
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
        public Builder withReenqueueSettings(@Nonnull ReenqueueSettings reenqueueSettings) {
            this.reenqueueSettings = reenqueueSettings;
            return this;
        }

        /**
         * Set the map of additional properties for the queue.
         *
         * @param extSettings Additional properties for the queue.
         * @return Reference to the same builder.
         */
        public Builder withExtSettings(@Nonnull ExtSettings extSettings) {
            this.extSettings = extSettings;
            return this;
        }

        /**
         * Create new queue settings object.
         *
         * @return A new queue settings object.
         */
        public QueueSettings build() {
            return new QueueSettings(processingSettings, pollSettings, failureSettings, reenqueueSettings, extSettings);
        }
    }
}
