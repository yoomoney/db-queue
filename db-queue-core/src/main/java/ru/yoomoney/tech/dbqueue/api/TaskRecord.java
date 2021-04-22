package ru.yoomoney.tech.dbqueue.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Raw database record with task parameters and payload
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public final class TaskRecord {
    private final long id;
    @Nullable
    private final String payload;
    private final long attemptsCount;
    private final long reenqueueAttemptsCount;
    private final long totalAttemptsCount;
    @Nonnull
    private final ZonedDateTime createdAt;
    @Nonnull
    private final ZonedDateTime nextProcessAt;
    @Nonnull
    private final Map<String, String> extData;

    /**
     * Constructor for raw database record with task parameters and payload.
     *
     * @param id                     Unique (sequence id) identifier of the task.
     * @param payload                Raw task payload.
     * @param attemptsCount          Number of attempts to execute the task.
     * @param reenqueueAttemptsCount Number of attempts to execute the task.
     * @param totalAttemptsCount     Sum of all attempts to execute the task.
     * @param createdAt              Date and time when the task was added into the queue.
     * @param nextProcessAt          Date and time of the next task execution.
     * @param extData                Map of external user-defined parameters, key is the column name in the tasks table.
     */
    private TaskRecord(long id,
                       @Nullable String payload,
                       long attemptsCount,
                       long reenqueueAttemptsCount,
                       long totalAttemptsCount,
                       @Nonnull ZonedDateTime createdAt,
                       @Nonnull ZonedDateTime nextProcessAt,
                       @Nonnull Map<String, String> extData) {
        this.id = id;
        this.payload = payload;
        this.attemptsCount = attemptsCount;
        this.reenqueueAttemptsCount = reenqueueAttemptsCount;
        this.totalAttemptsCount = totalAttemptsCount;
        this.createdAt = Objects.requireNonNull(createdAt);
        this.nextProcessAt = Objects.requireNonNull(nextProcessAt);
        this.extData = Objects.requireNonNull(extData);
    }

    /**
     * Get unique (sequence id) identifier of the task.
     *
     * @return task identifier
     */
    public long getId() {
        return id;
    }

    /**
     * Get raw task payload.
     *
     * @return task payload
     */
    @Nullable
    public String getPayload() {
        return payload;
    }

    /**
     * Get number of attempts to execute the task, including the current one.
     *
     * @return number of attempts to execute the task.
     */
    public long getAttemptsCount() {
        return attemptsCount;
    }

    /**
     * Get number of attempts to postpone (re-enqueue) the task.
     *
     * @return number of attempts to postpone (re-enqueue) the task.
     */
    public long getReenqueueAttemptsCount() {
        return reenqueueAttemptsCount;
    }

    /**
     * Get sum of all attempts to execute the task,
     * including all task re-enqueue attempts and all failed attempts.
     * <br>
     * <strong>This counter should never be reset.</strong>
     *
     * @return sum of all attempts to execute the task
     */
    public long getTotalAttemptsCount() {
        return totalAttemptsCount;
    }

    /**
     * Get date and time when the task was added into the queue.
     *
     * @return date and time when the task was added into the queue.
     */
    @Nonnull
    public ZonedDateTime getCreatedAt() {
        return createdAt;
    }

    /**
     * Get date and time of the next task execution.
     *
     * @return Date and time of the next task execution.
     */
    @Nonnull
    public ZonedDateTime getNextProcessAt() {
        return nextProcessAt;
    }

    /**
     * Get the map of external user-defined parameters,
     * where the key is the column name in the tasks table.
     *
     * @return map of external user-defined parameters, where the key is the column name in the tasks table.
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
        TaskRecord that = (TaskRecord) obj;
        return id == that.id &&
                attemptsCount == that.attemptsCount &&
                reenqueueAttemptsCount == that.reenqueueAttemptsCount &&
                totalAttemptsCount == that.totalAttemptsCount &&
                Objects.equals(payload, that.payload) &&
                Objects.equals(createdAt, that.createdAt) &&
                Objects.equals(nextProcessAt, that.nextProcessAt) &&
                Objects.equals(extData, that.extData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, payload, attemptsCount, reenqueueAttemptsCount, totalAttemptsCount,
                createdAt, nextProcessAt, extData);
    }

    @Override
    public String toString() {
        return '{' +
                "id=" + id +
                ", attemptsCount=" + attemptsCount +
                ", reenqueueAttemptsCount=" + reenqueueAttemptsCount +
                ", totalAttemptsCount=" + totalAttemptsCount +
                ", createdAt=" + createdAt +
                ", nextProcessAt=" + nextProcessAt +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for the {@link TaskRecord} class
     */
    public static class Builder {
        private long id;
        @Nullable
        private String payload;
        private long attemptsCount;
        private long reenqueueAttemptsCount;
        private long totalAttemptsCount;
        @Nonnull
        private ZonedDateTime createdAt = ZonedDateTime.now();
        @Nonnull
        private ZonedDateTime nextProcessAt = ZonedDateTime.now();
        @Nonnull
        private Map<String, String> extData = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder withCreatedAt(@Nonnull ZonedDateTime createdAt) {
            this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
            return this;
        }

        public Builder withNextProcessAt(@Nonnull ZonedDateTime nextProcessAt) {
            this.nextProcessAt = Objects.requireNonNull(nextProcessAt, "nextProcessAt");
            return this;
        }

        public Builder withId(long id) {
            this.id = id;
            return this;
        }

        public Builder withPayload(String payload) {
            this.payload = payload;
            return this;
        }

        public Builder withAttemptsCount(long attemptsCount) {
            this.attemptsCount = attemptsCount;
            return this;
        }

        public Builder withReenqueueAttemptsCount(long reenqueueAttemptsCount) {
            this.reenqueueAttemptsCount = reenqueueAttemptsCount;
            return this;
        }

        public Builder withTotalAttemptsCount(long totalAttemptsCount) {
            this.totalAttemptsCount = totalAttemptsCount;
            return this;
        }

        public Builder withExtData(@Nonnull Map<String, String> extData) {
            this.extData = Objects.requireNonNull(extData);
            return this;
        }

        public TaskRecord build() {
            return new TaskRecord(id, payload, attemptsCount, reenqueueAttemptsCount,
                    totalAttemptsCount, createdAt, nextProcessAt, extData);
        }
    }
}
