package ru.yandex.money.common.dbqueue.settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Settings for the task postponing strategy
 * when the task should be brought back to the queue.
 *
 * @author Dmitry Komarov
 * @since 21.05.2019
 */
public class ReenqueueRetrySettings {

    private static final Duration DEFAULT_INITIAL_DELAY = Duration.ofSeconds(1L);
    private static final Duration DEFAULT_ARITHMETIC_STEP = Duration.ofSeconds(2L);
    private static final long DEFAULT_GEOMETRIC_RATIO = 2L;

    /**
     * Strategy type, which computes delay to the next processing of the same task.
     * <p>
     * Required field.
     */
    @Nonnull
    private final ReenqueueRetryType type;

    /**
     * Sequence of delays for task processing.
     * <p>
     * Required when {@code type == ReenqueueRetryType.SEQUENTIAL}.
     */
    @Nullable
    private final List<Duration> sequentialPlan;

    /**
     * Fixed delay.
     * <p>
     * Required when {@code type == ReenqueueRetryType.FIXED}.
     */
    @Nullable
    private final Duration fixedDelay;

    /**
     * The first term of the progression to compute delays.
     * <p>
     * Required when {@code type == ReenqueueRetryType.ARITHMETIC} or {@code type == ReenqueueRetryType.GEOMETRIC}.
     */
    @Nonnull
    private final Duration initialDelay;

    /**
     * The difference of the arithmetic progression.
     * <p>
     * Required when {@code type == ReenqueueRetryType.ARITHMETIC}.
     */
    @Nonnull
    private final Duration arithmeticStep;

    /**
     * Denominator of the geometric progression.
     * <p>
     * Required when {@code type == ReenqueueRetryType.GEOMETRIC}.
     */
    private final long geometricRatio;

    private ReenqueueRetrySettings(@Nonnull ReenqueueRetryType type,
                                   @Nullable List<Duration> sequentialPlan,
                                   @Nullable Duration fixedDelay,
                                   @Nullable Duration initialDelay,
                                   @Nullable Duration arithmeticStep,
                                   @Nullable Long geometricRatio) {
        this.type = Objects.requireNonNull(type, "type");
        this.sequentialPlan = sequentialPlan;
        this.fixedDelay = fixedDelay;
        this.initialDelay = initialDelay == null ? DEFAULT_INITIAL_DELAY : initialDelay;
        this.arithmeticStep = arithmeticStep == null ? DEFAULT_ARITHMETIC_STEP : arithmeticStep;
        this.geometricRatio = geometricRatio == null ? DEFAULT_GEOMETRIC_RATIO : geometricRatio;
    }

    @Nonnull
    public ReenqueueRetryType getType() {
        return type;
    }

    /**
     * Get the sequential plan of delays for task processing or throw an exception when plan is not present.
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
     * Get fixed delay.
     *
     * @return Fixed delay.
     * @see IllegalStateException when fixed delay is not present.
     */
    @Nonnull
    public Duration getFixedDelayOrThrow() {
        if (fixedDelay == null) {
            throw new IllegalStateException("fixed delay is null");
        }
        return fixedDelay;
    }

    @Nonnull
    public Duration getInitialDelay() {
        return initialDelay;
    }

    @Nonnull
    public Duration getArithmeticStep() {
        return arithmeticStep;
    }

    public long getGeometricRatio() {
        return geometricRatio;
    }

    @Override
    public String toString() {
        return "ReenqueueRetrySettings{" +
                "type=" + type +
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
        ReenqueueRetrySettings that = (ReenqueueRetrySettings) obj;
        return geometricRatio == that.geometricRatio &&
                type == that.type &&
                Objects.equals(sequentialPlan, that.sequentialPlan) &&
                Objects.equals(fixedDelay, that.fixedDelay) &&
                Objects.equals(initialDelay, that.initialDelay) &&
                Objects.equals(arithmeticStep, that.arithmeticStep);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, sequentialPlan, fixedDelay, initialDelay, arithmeticStep, geometricRatio);
    }

    @Nonnull
    public static Builder builder(@Nonnull ReenqueueRetryType type) {
        return new Builder(type);
    }

    @Nonnull
    public static ReenqueueRetrySettings createDefault() {
        return builder(ReenqueueRetryType.MANUAL).build();
    }

    /**
     * A builder for creating new instances of {@link ReenqueueRetrySettings}.
     */
    public static class Builder {

        private ReenqueueRetryType type;
        private List<Duration> sequentialPlan;
        private Duration fixedDelay;
        private Duration initialDelay;
        private Duration arithmeticStep;
        private Long geometricRatio;

        private Builder(@Nonnull ReenqueueRetryType type) {
            this.type = type;
        }

        @Nonnull
        public Builder withSequentialPlan(@Nonnull List<Duration> sequentialPlan) {
            this.sequentialPlan = sequentialPlan;
            return this;
        }

        @Nonnull
        public Builder withFixedDelay(@Nonnull Duration fixedDelay) {
            this.fixedDelay = fixedDelay;
            return this;
        }

        @Nonnull
        public Builder withInitialDelay(@Nonnull Duration initialDelay) {
            this.initialDelay = initialDelay;
            return this;
        }

        @Nonnull
        public Builder withArithmeticStep(@Nonnull Duration arithmeticStep) {
            this.arithmeticStep = arithmeticStep;
            return this;
        }

        @Nonnull
        public Builder withGeometricRatio(@Nonnull Long geometricRatio) {
            this.geometricRatio = geometricRatio;
            return this;
        }

        @Nonnull
        public ReenqueueRetrySettings build() {
            return new ReenqueueRetrySettings(type, sequentialPlan, fixedDelay, initialDelay, arithmeticStep, geometricRatio);
        }
    }
}
