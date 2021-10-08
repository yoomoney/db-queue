package ru.yoomoney.tech.dbqueue.settings;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.BiFunction;

/**
 * Task processing settings.
 *
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
public class ProcessingSettings extends DynamicSetting<ProcessingSettings> {
    @Nonnull
    private Integer threadCount;
    @Nonnull
    private ProcessingMode processingMode;

    private ProcessingSettings(@Nonnull Integer threadCount,
                               @Nonnull ProcessingMode processingMode) {
        this.threadCount = Objects.requireNonNull(threadCount, "threadCount must not be null");
        this.processingMode = Objects.requireNonNull(processingMode, "processingMode must not be null");
        if (threadCount < 0) {
            throw new IllegalArgumentException("threadCount must not be negative");
        }
    }

    /**
     * Get number of threads for processing tasks in the queue.
     *
     * @return Number of processing threads.
     */
    @Nonnull
    public Integer getThreadCount() {
        return threadCount;
    }

    /**
     * Get task processing mode in the queue.
     *
     * @return Task processing mode.
     */
    @Nonnull
    public ProcessingMode getProcessingMode() {
        return processingMode;
    }

    /**
     * Create a new builder for processing settings.
     *
     * @return A new builder for processing settings.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Nonnull
    @Override
    protected String getName() {
        return "processingSettings";
    }

    @Nonnull
    @Override
    protected BiFunction<ProcessingSettings, ProcessingSettings, String> getDiffEvaluator() {
        return (oldVal, newVal) -> {
            StringJoiner diff = new StringJoiner(",", getName() + '(', ")");
            if (!Objects.equals(oldVal.threadCount, newVal.threadCount)) {
                diff.add("threadCount=" +
                        newVal.threadCount + '<' + oldVal.threadCount);
            }
            if (!Objects.equals(oldVal.processingMode, newVal.processingMode)) {
                diff.add("processingMode=" +
                        newVal.processingMode + '<' + oldVal.processingMode);
            }
            return diff.toString();
        };
    }

    @Nonnull
    @Override
    protected ProcessingSettings getThis() {
        return this;
    }

    @Override
    protected void copyFields(@Nonnull ProcessingSettings newValue) {
        this.threadCount = newValue.threadCount;
        this.processingMode = newValue.processingMode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ProcessingSettings that = (ProcessingSettings) obj;
        return threadCount.equals(that.threadCount) && processingMode == that.processingMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(threadCount, processingMode);
    }

    @Override
    public String toString() {
        return "{" +
                "threadCount=" + threadCount +
                ", processingMode=" + processingMode +
                '}';
    }

    /**
     * A builder for processing settings.
     */
    public static class Builder {
        private Integer threadCount;
        private ProcessingMode processingMode;

        private Builder() {
        }

        /**
         * Set number of threads for processing tasks in the queue.
         *
         * @param threadCount Number of processing threads.
         * @return Reference to the same builder.
         */
        public Builder withThreadCount(@Nonnull Integer threadCount) {
            this.threadCount = threadCount;
            return this;
        }

        /**
         * Set task processing mode in the queue.
         *
         * @param processingMode Task processing mode.
         * @return Reference to the same builder.
         */
        public Builder withProcessingMode(@Nonnull ProcessingMode processingMode) {
            this.processingMode = processingMode;
            return this;
        }

        public ProcessingSettings build() {
            return new ProcessingSettings(threadCount, processingMode);
        }
    }
}
