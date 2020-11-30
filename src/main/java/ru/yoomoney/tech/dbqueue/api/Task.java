package ru.yoomoney.tech.dbqueue.api;

import ru.yoomoney.tech.dbqueue.config.QueueShardId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Typed task wrapper with parameters, which is supplied to the {@linkplain QueueConsumer} task processor
 *
 * @param <T> The type of the payload in the task
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public final class Task<T> {

    @Nonnull
    private final QueueShardId shardId;
    @Nullable
    private final T payload;
    private final long attemptsCount;
    private final long reenqueueAttemptsCount;
    private final long totalAttemptsCount;
    @Nonnull
    private final ZonedDateTime createdAt;
    @Nonnull
    private final Map<String, String> extData;

    /**
     * Constructor of typed task wrapper with task parameters.
     *
     * @param shardId                Shard identifier from which the task executor took the task.
     * @param payload                Task payload.
     * @param attemptsCount          Number of attempts to execute the task, including the current one.
     * @param reenqueueAttemptsCount Number of attempts to postpone (re-enqueue) the task.
     * @param totalAttemptsCount     Sum of all attempts to execute the task,
     *                               including all task re-enqueue attempts and all failed attempts.
     * @param createdAt              Date and time when the task was added into the queue.
     * @param extData                Map of external user-defined parameters, key is the column name in the tasks table.
     */
    private Task(@Nonnull QueueShardId shardId, @Nullable T payload,
                 long attemptsCount, long reenqueueAttemptsCount, long totalAttemptsCount,
                 @Nonnull ZonedDateTime createdAt, @Nonnull Map<String, String> extData) {
        this.shardId = requireNonNull(shardId, "shardId");
        this.payload = payload;
        this.attemptsCount = attemptsCount;
        this.reenqueueAttemptsCount = reenqueueAttemptsCount;
        this.totalAttemptsCount = totalAttemptsCount;
        this.createdAt = requireNonNull(createdAt, "createdAt");
        this.extData = requireNonNull(extData, "extData");
    }

    /**
     * Get typed task payload.
     *
     * @return Typed task payload.
     */
    @Nonnull
    public Optional<T> getPayload() {
        return Optional.ofNullable(payload);
    }

    /**
     * Get typed task payload or throw {@linkplain IllegalArgumentException} if not present.
     *
     * @return Typed task payload.
     */
    @Nonnull
    public T getPayloadOrThrow() {
        if (payload == null) {
            throw new IllegalArgumentException("payload is absent");
        }
        return payload;
    }

    /**
     * Get number of attempts to execute the task, including the current one.
     *
     * @return Number of attempts to execute the task.
     */
    public long getAttemptsCount() {
        return attemptsCount;
    }

    /**
     * Get number of attempts to postpone (re-enqueue) the task.
     *
     * @return Number of attempts to postpone (re-enqueue) the task.
     */
    public long getReenqueueAttemptsCount() {
        return reenqueueAttemptsCount;
    }

    /**
     * Get sum of all attempts to execute the task, including all task re-enqueue attempts and all failed attempts.
     * <br>
     * <strong>This counter should never be reset.</strong>
     *
     * @return Sum of all attempts to execute the task.
     */
    public long getTotalAttemptsCount() {
        return totalAttemptsCount;
    }

    /**
     * Get date and time when the task was added into the queue.
     *
     * @return Date and time when the task was added into the queue.
     */
    @Nonnull
    public ZonedDateTime getCreatedAt() {
        return createdAt;
    }

    /**
     * Get the shard identifier from which the task executor took the task.
     *
     * @return Shard identifier from which the task executor took the task.
     */
    @Nonnull
    public QueueShardId getShardId() {
        return shardId;
    }

    /**
     * Get the map of external user-defined parameters, where the key is the column name in the tasks table.
     *
     * @return Map of external user-defined parameters, where the key is the column name in the tasks table.
     */
    @Nonnull
    public Map<String, String> getExtData() {
        return extData;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Task<?> task = (Task<?>) obj;
        return attemptsCount == task.attemptsCount &&
                reenqueueAttemptsCount == task.reenqueueAttemptsCount &&
                totalAttemptsCount == task.totalAttemptsCount &&
                Objects.equals(shardId, task.shardId) &&
                Objects.equals(payload, task.payload) &&
                Objects.equals(createdAt, task.createdAt) &&
                Objects.equals(extData, task.extData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, payload, attemptsCount, reenqueueAttemptsCount,
                totalAttemptsCount, createdAt, extData);
    }

    @Override
    public String toString() {
        return '{' +
                "shardId=" + shardId +
                ", attemptsCount=" + attemptsCount +
                ", reenqueueAttemptsCount=" + reenqueueAttemptsCount +
                ", totalAttemptsCount=" + totalAttemptsCount +
                ", createdAt=" + createdAt +
                ", payload=" + payload +
                '}';
    }

    /**
     * Creates a builder for {@link Task} objects.
     *
     * @param shardId An id of shard.
     * @param <T>     A type of task payload.
     * @return A new instance of the {@link Builder} builder.
     */
    public static <T> Builder<T> builder(@Nonnull QueueShardId shardId) {
        return new Builder<>(shardId);
    }

    /**
     * Builder for the {@link Task} wrapper.
     *
     * @param <T> The type of the payload in the task.
     */
    public static class Builder<T> {
        @Nonnull
        private final QueueShardId shardId;
        @Nonnull
        private ZonedDateTime createdAt = ZonedDateTime.now();
        private T payload;
        private long attemptsCount;
        private long reenqueueAttemptsCount;
        private long totalAttemptsCount;
        @Nonnull
        private Map<String, String> extData = new LinkedHashMap<>();

        private Builder(@Nonnull QueueShardId shardId) {
            this.shardId = requireNonNull(shardId, "shardId");
        }

        public Builder<T> withCreatedAt(@Nonnull ZonedDateTime createdAt) {
            this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
            return this;
        }

        public Builder<T> withPayload(T payload) {
            this.payload = payload;
            return this;
        }

        public Builder<T> withAttemptsCount(long attemptsCount) {
            this.attemptsCount = attemptsCount;
            return this;
        }

        public Builder<T> withReenqueueAttemptsCount(long reenqueueAttemptsCount) {
            this.reenqueueAttemptsCount = reenqueueAttemptsCount;
            return this;
        }

        public Builder<T> withTotalAttemptsCount(long totalAttemptsCount) {
            this.totalAttemptsCount = totalAttemptsCount;
            return this;
        }

        public Builder<T> withExtData(@Nonnull Map<String, String> extData) {
            this.extData = requireNonNull(extData);
            return this;
        }

        public Task<T> build() {
            return new Task<>(shardId, payload, attemptsCount, reenqueueAttemptsCount,
                    totalAttemptsCount, createdAt, extData);
        }
    }
}
