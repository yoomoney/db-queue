package ru.yandex.money.common.dbqueue.settings;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Queue location in the database.
 *
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public final class QueueLocation {

    /**
     * Regexp for SQL injection prevention
     */
    private static final Pattern DISALLOWED_CHARS = Pattern.compile("[^a-zA-Z0-9_\\.]*");

    @Nonnull
    private final String tableName;
    @Nonnull
    private final QueueId queueId;

    private QueueLocation(@Nonnull QueueId queueId, @Nonnull String tableName) {
        this.queueId = Objects.requireNonNull(queueId);
        this.tableName = DISALLOWED_CHARS.matcher(Objects.requireNonNull(tableName)).replaceAll("");
    }

    /**
     * Get queue table name.
     *
     * @return Table name.
     */
    @Nonnull
    public String getTableName() {
        return tableName;
    }

    /**
     * Get queue identifier.
     *
     * @return Queue identifier.
     */
    @Nonnull
    public QueueId getQueueId() {
        return queueId;
    }

    @Override
    public String toString() {
        return '{' +
                "id=" + queueId +
                ",table=" + tableName +
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
        QueueLocation that = (QueueLocation) obj;
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(queueId, that.queueId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, queueId);
    }

    /**
     * Create a new builder for queue location.
     *
     * @return A builder for queue location.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for class {@link QueueLocation}.
     */
    public static class Builder {
        private String tableName;
        private QueueId queueId;

        private Builder() {
        }

        /**
         * Set table name for queue tasks.
         *
         * @param tableName Table name.
         * @return Reference to the same builder.
         */
        public Builder withTableName(@Nonnull String tableName) {
            this.tableName = Objects.requireNonNull(tableName);
            return this;
        }

        /**
         * Set queue identifier.
         *
         * @param queueId Queue identifier.
         * @return Reference to the same builder.
         */
        public Builder withQueueId(@Nonnull QueueId queueId) {
            this.queueId = Objects.requireNonNull(queueId);
            return this;
        }

        /**
         * Build queue location object.
         *
         * @return Queue location  object.
         */
        public QueueLocation build() {
            return new QueueLocation(queueId, tableName);
        }
    }
}
