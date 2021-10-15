package ru.yoomoney.tech.dbqueue.settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.BiFunction;

/**
 * Settings for the task postponing strategy
 * when the task should be brought back to the queue.
 *
 * @author Dmitry Komarov
 * @since 21.05.2019
 */
public class ReenqueueSettings extends DynamicSetting<ReenqueueSettings> {

    @Nonnull
    private ReenqueueRetryType retryType;

    @Nullable
    private List<Duration> sequentialPlan;

    @Nullable
    private Duration fixedDelay;

    @Nullable
    private Duration initialDelay;

    @Nullable
    private Duration arithmeticStep;

    @Nullable
    private Long geometricRatio;

    private ReenqueueSettings(@Nonnull ReenqueueRetryType retryType,
                              @Nullable List<Duration> sequentialPlan,
                              @Nullable Duration fixedDelay,
                              @Nullable Duration initialDelay,
                              @Nullable Duration arithmeticStep,
                              @Nullable Long geometricRatio) {
        this.retryType = Objects.requireNonNull(retryType, "retryType must not be null");
        this.sequentialPlan = sequentialPlan;
        this.fixedDelay = fixedDelay;
        this.initialDelay = initialDelay;
        this.arithmeticStep = arithmeticStep;
        this.geometricRatio = geometricRatio;
        if (retryType == ReenqueueRetryType.SEQUENTIAL && (sequentialPlan == null || sequentialPlan.isEmpty())) {
            throw new IllegalArgumentException(
                    "sequentialPlan must not be empty when retryType=" + ReenqueueRetryType.SEQUENTIAL);
        }
        if (retryType == ReenqueueRetryType.FIXED && fixedDelay == null) {
            throw new IllegalArgumentException(
                    "fixedDelay must not be empty when retryType=" + ReenqueueRetryType.FIXED);
        }
        if (retryType == ReenqueueRetryType.ARITHMETIC && (arithmeticStep == null || initialDelay == null)) {
            throw new IllegalArgumentException(
                    "arithmeticStep and initialDelay must not be empty when retryType=" + ReenqueueRetryType.ARITHMETIC);
        }
        if (retryType == ReenqueueRetryType.GEOMETRIC && (geometricRatio == null || initialDelay == null)) {
            throw new IllegalArgumentException(
                    "geometricRatio and initialDelay must not be empty when retryType=" + ReenqueueRetryType.GEOMETRIC);
        }
    }

    /**
     * Strategy type, which computes delay to the next processing of the same task.
     *
     * @return reenqueue retry type
     */
    @Nonnull
    public ReenqueueRetryType getRetryType() {
        return retryType;
    }

    /**
     * Get the sequential plan of delays for task processing.
     * <p>
     * Required when {@code type == ReenqueueRetryType.SEQUENTIAL}.
     *
     * @return Sequential plan of delays
     * @throws IllegalStateException when plan is not present.
     */
    @Nonnull
    public List<Duration> getSequentialPlanOrThrow() {
        if (sequentialPlan == null) {
            throw new IllegalStateException("sequential plan is null");
        }
        return sequentialPlan;
    }

    /**
     * Fixed delay.
     * <p>
     * Required when {@code type == ReenqueueRetryType.FIXED}.
     *
     * @return Fixed delay.
     * @throws IllegalStateException when fixed delay is not present.
     */
    @Nonnull
    public Duration getFixedDelayOrThrow() {
        if (fixedDelay == null) {
            throw new IllegalStateException("fixed delay is null");
        }
        return fixedDelay;
    }

    /**
     * The first term of the progression to compute delays.
     * <p>
     * Required when {@code type == ReenqueueRetryType.ARITHMETIC} or {@code type == ReenqueueRetryType.GEOMETRIC}.
     *
     * @return initial delay
     * @throws IllegalStateException when initial delay is not present.
     */
    @Nonnull
    public Duration getInitialDelayOrThrow() {
        if (initialDelay == null) {
            throw new IllegalStateException("initial delay is null");
        }
        return initialDelay;
    }

    /**
     * The difference of the arithmetic progression.
     * <p>
     * Required when {@code type == ReenqueueRetryType.ARITHMETIC}.
     *
     * @return arithmetic step
     * @throws IllegalStateException when artithmetic step is not present.
     */
    @Nonnull
    public Duration getArithmeticStepOrThrow() {
        if (arithmeticStep == null) {
            throw new IllegalStateException("arithmetic step is null");
        }
        return arithmeticStep;
    }

    /**
     * Denominator of the geometric progression.
     * <p>
     * Required when {@code type == ReenqueueRetryType.GEOMETRIC}.
     *
     * @return geometric ratio
     * @throws IllegalStateException when geometric ratio is not present.
     */
    @Nonnull
    public Long getGeometricRatioOrThrow() {
        if (geometricRatio == null) {
            throw new IllegalStateException("geometric ratio is null");
        }
        return geometricRatio;
    }

    @Override
    public String toString() {
        return "{" +
                "retryType=" + retryType +
                ", sequentialPlan=" + sequentialPlan +
                ", fixedDelay=" + fixedDelay +
                ", initialDelay=" + initialDelay +
                ", arithmeticStep=" + arithmeticStep +
                ", geometricRatio=" + geometricRatio +
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
        ReenqueueSettings that = (ReenqueueSettings) obj;
        return retryType == that.retryType && Objects.equals(sequentialPlan, that.sequentialPlan) &&
                Objects.equals(fixedDelay, that.fixedDelay) &&
                Objects.equals(initialDelay, that.initialDelay) &&
                Objects.equals(arithmeticStep, that.arithmeticStep) &&
                Objects.equals(geometricRatio, that.geometricRatio);
    }

    @Override
    public int hashCode() {
        return Objects.hash(retryType, sequentialPlan, fixedDelay, initialDelay, arithmeticStep, geometricRatio);
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    @Nonnull
    @Override
    protected String getName() {
        return "reenqueueSettings";
    }

    @Nonnull
    @Override
    protected BiFunction<ReenqueueSettings, ReenqueueSettings, String> getDiffEvaluator() {
        return (oldVal, newVal) -> {
            StringJoiner diff = new StringJoiner(",", getName() + '(', ")");
            if (!Objects.equals(oldVal.retryType, newVal.retryType)) {
                diff.add("type=" +
                        newVal.retryType + '<' + oldVal.retryType);
            }
            if (!Objects.equals(oldVal.arithmeticStep, newVal.arithmeticStep)) {
                diff.add("arithmeticStep=" +
                        newVal.arithmeticStep + '<' + oldVal.arithmeticStep);
            }
            if (!Objects.equals(oldVal.geometricRatio, newVal.geometricRatio)) {
                diff.add("geometricRatio=" +
                        newVal.geometricRatio + '<' + oldVal.geometricRatio);
            }
            if (!Objects.equals(oldVal.initialDelay, newVal.initialDelay)) {
                diff.add("initialDelay=" +
                        newVal.initialDelay + '<' + oldVal.initialDelay);
            }
            if (!Objects.equals(oldVal.fixedDelay, newVal.fixedDelay)) {
                diff.add("fixedDelay=" +
                        newVal.fixedDelay + '<' + oldVal.fixedDelay);
            }
            if (!Objects.equals(oldVal.sequentialPlan, newVal.sequentialPlan)) {
                diff.add("sequentialPlan=" +
                        newVal.sequentialPlan + '<' + oldVal.sequentialPlan);
            }
            return diff.toString();
        };
    }

    @Nonnull
    @Override
    protected ReenqueueSettings getThis() {
        return this;
    }

    @Override
    protected void copyFields(@Nonnull ReenqueueSettings newValue) {
        this.retryType = newValue.retryType;
        this.arithmeticStep = newValue.arithmeticStep;
        this.geometricRatio = newValue.geometricRatio;
        this.fixedDelay = newValue.fixedDelay;
        this.initialDelay = newValue.initialDelay;
        this.sequentialPlan = newValue.sequentialPlan;
    }

    /**
     * A builder for creating new instances of {@link ReenqueueSettings}.
     */
    public static class Builder {

        private ReenqueueRetryType retryType;
        private List<Duration> sequentialPlan;
        private Duration fixedDelay;
        private Duration initialDelay;
        private Duration arithmeticStep;
        private Long geometricRatio;

        @Nonnull
        public Builder withRetryType(@Nonnull ReenqueueRetryType retryType) {
            this.retryType = retryType;
            return this;
        }

        @Nonnull
        public Builder withSequentialPlan(@Nullable List<Duration> sequentialPlan) {
            this.sequentialPlan = sequentialPlan;
            return this;
        }

        @Nonnull
        public Builder withFixedDelay(@Nullable Duration fixedDelay) {
            this.fixedDelay = fixedDelay;
            return this;
        }

        @Nonnull
        public Builder withInitialDelay(@Nullable Duration initialDelay) {
            this.initialDelay = initialDelay;
            return this;
        }

        @Nonnull
        public Builder withArithmeticStep(@Nullable Duration arithmeticStep) {
            this.arithmeticStep = arithmeticStep;
            return this;
        }

        @Nonnull
        public Builder withGeometricRatio(@Nullable Long geometricRatio) {
            this.geometricRatio = geometricRatio;
            return this;
        }

        @Nonnull
        public ReenqueueSettings build() {
            return new ReenqueueSettings(retryType, sequentialPlan, fixedDelay, initialDelay, arithmeticStep, geometricRatio);
        }
    }
}
