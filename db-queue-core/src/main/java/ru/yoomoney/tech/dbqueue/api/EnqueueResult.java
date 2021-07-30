package ru.yoomoney.tech.dbqueue.api;

import ru.yoomoney.tech.dbqueue.config.QueueShardId;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Task enqueue result
 *
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class EnqueueResult {
    @Nonnull
    private final QueueShardId shardId;
    @Nonnull
    private final Long enqueueId;

    /**
     * Constructor
     *
     * @param shardId   shard id
     * @param enqueueId sequence id
     */
    public EnqueueResult(@Nonnull QueueShardId shardId, @Nonnull Long enqueueId) {
        this.shardId = Objects.requireNonNull(shardId);
        this.enqueueId = Objects.requireNonNull(enqueueId);
    }

    /**
     * Shard identifier of added task
     *
     * @return shard id
     */
    @Nonnull
    public QueueShardId getShardId() {
        return shardId;
    }

    /**
     * Identifier (sequence id) of added task
     *
     * @return sequence id
     */
    @Nonnull
    public Long getEnqueueId() {
        return enqueueId;
    }

    @Override
    public String toString() {
        return "EnqueueResult{" +
                "shardId=" + shardId +
                ", enqueueId=" + enqueueId +
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
        EnqueueResult that = (EnqueueResult) obj;
        return shardId.equals(that.shardId) && enqueueId.equals(that.enqueueId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, enqueueId);
    }

    /**
     * Creates builder for {@link EnqueueResult} object
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for the {@link EnqueueResult} object.
     */
    public static class Builder {

        private QueueShardId shardId;
        private Long enqueueId;

        private Builder() {
        }

        /**
         * Set shard identifier of added task
         *
         * @param shardId shard identifier of added task
         * @return Builder
         */
        public Builder withShardId(@Nonnull QueueShardId shardId) {
            this.shardId = Objects.requireNonNull(shardId, "shardId must not be null");
            return this;
        }

        /**
         * Set identifier (sequence id) of added task
         *
         * @param enqueueId sequence id
         * @return Builder
         */
        public Builder withEnqueueId(@Nonnull Long enqueueId) {
            this.enqueueId = Objects.requireNonNull(enqueueId, "enqueueId must not be null");
            return this;
        }

        public EnqueueResult build() {
            return new EnqueueResult(shardId, enqueueId);
        }
    }
}
