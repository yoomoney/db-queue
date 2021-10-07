package ru.yoomoney.tech.dbqueue.settings;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

/**
 * Settings for task execution strategy in case of failure.
 *
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
public class FailureSettings extends DynamicSetting<FailureSettings> {
    @Nonnull
    private FailRetryType retryType;
    @Nonnull
    private Duration retryInterval;

    private FailureSettings(@Nonnull FailRetryType retryType,
                            @Nonnull Duration retryInterval) {
        this.retryType = requireNonNull(retryType, "retryType must not be null");
        this.retryInterval = requireNonNull(retryInterval, "retryInterval must not be null");
    }

    /**
     * Get task execution retry strategy in case of failure.
     *
     * @return Task execution retry strategy.
     */
    @Nonnull
    public FailRetryType getRetryType() {
        return retryType;
    }

    /**
     * Get retry interval for task execution in case of failure.
     *
     * @return Task retry interval.
     * @see FailRetryType
     */
    @Nonnull
    public Duration getRetryInterval() {
        return retryInterval;
    }

    /**
     * Create a new builder for failure settings.
     *
     * @return A new builder for failure settings.
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
        FailureSettings that = (FailureSettings) obj;
        return retryType == that.retryType && retryInterval.equals(that.retryInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(retryType, retryInterval);
    }

    @Override
    public String toString() {
        return "{" +
                "retryType=" + retryType +
                ", retryInterval=" + retryInterval +
                '}';
    }

    @Nonnull
    @Override
    protected String getName() {
        return "failureSettings";
    }

    @Nonnull
    @Override
    protected BiFunction<FailureSettings, FailureSettings, String> getDiffEvaluator() {
        return (oldVal, newVal) -> {
            StringJoiner diff = new StringJoiner(",", getName() + "(", ")");
            if (!Objects.equals(oldVal.retryType, newVal.retryType)) {
                diff.add("type=" +
                        newVal.retryType + "<-" + oldVal.retryType);
            }
            if (!Objects.equals(oldVal.retryInterval, newVal.retryInterval)) {
                diff.add("interval=" +
                        newVal.retryInterval + "<-" + oldVal.retryInterval);
            }
            return diff.toString();
        };
    }

    @Nonnull
    @Override
    protected FailureSettings getThis() {
        return this;
    }

    @Override
    protected void copyFields(@Nonnull FailureSettings newValue) {
        this.retryType = newValue.retryType;
        this.retryInterval = newValue.retryInterval;
    }

    /**
     * A builder for failure settings.
     */
    public static class Builder {
        private FailRetryType retryType;
        private Duration retryInterval;

        /**
         * Set task execution retry strategy in case of failure.
         *
         * @param retryType Task execution retry strategy.
         * @return Reference to the same builder.
         */
        public Builder withRetryType(@Nonnull FailRetryType retryType) {
            this.retryType = retryType;
            return this;
        }

        /**
         * Set retry interval for task execution in case of failure.
         *
         * @param retryInterval Task retry interval.
         * @return Reference to the same builder.
         */
        public Builder withRetryInterval(@Nonnull Duration retryInterval) {
            this.retryInterval = retryInterval;
            return this;
        }

        /**
         * Create new failure settings object.
         *
         * @return A new failure settings object.
         */
        public FailureSettings build() {
            return new FailureSettings(retryType, retryInterval);
        }
    }
}
