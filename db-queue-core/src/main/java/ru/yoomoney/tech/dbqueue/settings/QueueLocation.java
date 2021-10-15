package ru.yoomoney.tech.dbqueue.settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
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
    @Nullable
    private final String idSequence;

    private QueueLocation(@Nonnull QueueId queueId, @Nonnull String tableName,
                          @Nullable String idSequence) {
        this.queueId = Objects.requireNonNull(queueId, "queueId must not be null");
        this.tableName = DISALLOWED_CHARS.matcher(
                Objects.requireNonNull(tableName, "tableName must not be null")).replaceAll("");
        this.idSequence = idSequence != null ? DISALLOWED_CHARS.matcher(idSequence).replaceAll("") : null;
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

    /**
     * Get id sequence name.
     * <p>
     * Use for databases which doesn't have automatically incremented primary keys, for example Oracle 11g
     *
     * @return database sequence name for generating primary key of tasks table.
     */
    public Optional<String> getIdSequence() {
        return Optional.ofNullable(idSequence);
    }

    @Override
    public String toString() {
        return '{' +
                "id=" + queueId +
                ",table=" + tableName +
                (idSequence != null ? ",idSequence=" + idSequence : "") +
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
                Objects.equals(queueId, that.queueId) &&
                Objects.equals(idSequence, that.idSequence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, queueId, idSequence);
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
        @Nullable
        private String idSequence;

        private Builder() {
        }

        /**
         * Set table name for queue tasks.
         *
         * @param tableName Table name.
         * @return Reference to the same builder.
         */
        public Builder withTableName(@Nonnull String tableName) {
            this.tableName = tableName;
            return this;
        }

        /**
         * Set queue identifier.
         *
         * @param queueId Queue identifier.
         * @return Reference to the same builder.
         */
        public Builder withQueueId(@Nonnull QueueId queueId) {
            this.queueId = queueId;
            return this;
        }

        /**
         * Set id sequence name.
         *
         * @param idSequence database sequence name for generating primary key of tasks table.
         * @return Reference to the same builder.
         */
        public Builder withIdSequence(@Nullable String idSequence) {
            this.idSequence = idSequence;
            return this;
        }

        /**
         * Build queue location object.
         *
         * @return Queue location  object.
         */
        public QueueLocation build() {
            return new QueueLocation(queueId, tableName, idSequence);
        }
    }
}
