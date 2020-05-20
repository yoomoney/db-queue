package ru.yandex.money.common.dbqueue.config;

import ru.yandex.money.common.dbqueue.api.TaskRecord;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Scheme for column names of queue table in the database.
 *
 * @author Oleg Kandaurov
 * @since 05.10.2019
 */
public class QueueTableSchema {

    /**
     * Regexp for SQL injection prevention
     */
    private static final Pattern DISALLOWED_CHARS = Pattern.compile("[^a-zA-Z0-9_]*");

    @Nonnull
    private final String idField;
    @Nonnull
    private final String queueNameField;
    @Nonnull
    private final String payloadField;
    @Nonnull
    private final String attemptField;
    @Nonnull
    private final String reenqueueAttemptField;
    @Nonnull
    private final String totalAttemptField;
    @Nonnull
    private final String createdAtField;
    @Nonnull
    private final String nextProcessAtField;
    @Nonnull
    private final List<String> extFields;

    private QueueTableSchema(@Nonnull String idField,
                             @Nonnull String queueNameField,
                             @Nonnull String payloadField,
                             @Nonnull String attemptField,
                             @Nonnull String reenqueueAttemptField,
                             @Nonnull String totalAttemptField,
                             @Nonnull String createdAtField,
                             @Nonnull String nextProcessAtField,
                             @Nonnull List<String> extFields) {
        this.idField = removeSpecialChars(requireNonNull(idField));
        this.queueNameField = removeSpecialChars(requireNonNull(queueNameField));
        this.payloadField = removeSpecialChars(requireNonNull(payloadField));
        this.attemptField = removeSpecialChars(requireNonNull(attemptField));
        this.reenqueueAttemptField = removeSpecialChars(requireNonNull(reenqueueAttemptField));
        this.totalAttemptField = removeSpecialChars(requireNonNull(totalAttemptField));
        this.createdAtField = removeSpecialChars(requireNonNull(createdAtField));
        this.nextProcessAtField = removeSpecialChars(requireNonNull(nextProcessAtField));
        this.extFields = requireNonNull(extFields).stream().map(QueueTableSchema::removeSpecialChars)
                .collect(Collectors.toList());
    }

    /**
     * Delete special chars to prevent SQL injection
     *
     * @param value input string
     * @return string without special chars
     */
    private static String removeSpecialChars(@Nonnull String value) {
        return DISALLOWED_CHARS.matcher(value).replaceAll("");
    }

    /**
     * Field with a column name for task payload.
     * Column maps onto {@link TaskRecord#getPayload()}.
     *
     * @return Column name.
     */
    @Nonnull
    public String getPayloadField() {
        return payloadField;
    }

    /**
     * Field with a column name for task execution attempts count.
     * Column maps onto {@link TaskRecord#getAttemptsCount()}.
     *
     * @return Column name.
     */
    @Nonnull
    public String getAttemptField() {
        return attemptField;
    }

    /**
     * Field with a column name for task execution re-enqueue attempts count.
     * Column maps onto {@link TaskRecord#getReenqueueAttemptsCount()}.
     *
     * @return Column name.
     */
    @Nonnull
    public String getReenqueueAttemptField() {
        return reenqueueAttemptField;
    }

    /**
     * Field with a column name for task execution total attempts count.
     * Column maps onto {@link TaskRecord#getTotalAttemptsCount()}.
     *
     * @return Column name.
     */
    @Nonnull
    public String getTotalAttemptField() {
        return totalAttemptField;
    }

    /**
     * Field with a column name for task creation date and time.
     * Column maps onto {@link TaskRecord#getCreatedAt()}.
     *
     * @return Column name.
     */
    @Nonnull
    public String getCreatedAtField() {
        return createdAtField;
    }

    /**
     * Field with a column name for task processing date and time (task will be processed after this date).
     *
     * @return Column name.
     */
    @Nonnull
    public String getNextProcessAtField() {
        return nextProcessAtField;
    }

    /**
     * Field with a column name for the queue name.
     *
     * @return Column name.
     */
    @Nonnull
    public String getQueueNameField() {
        return queueNameField;
    }

    /**
     * Field with a column name for the id.
     *
     * @return Column name.
     */
    @Nonnull
    public String getIdField() {
        return idField;
    }

    /**
     * Additional list of column names ({@code TEXT} type),
     * which are mapping onto {@link TaskRecord#getExtData()}.
     *
     * @return List of column names.
     */
    @Nonnull
    public List<String> getExtFields() {
        return extFields;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link QueueTableSchema} class.
     */
    public static class Builder {
        private String idField = "id";
        private String queueNameField = "queue_name";
        private String payloadField = "payload";
        private String attemptField = "attempt";
        private String reenqueueAttemptField = "reenqueue_attempt";
        private String totalAttemptField = "total_attempt";
        private String createdAtField = "created_at";
        private String nextProcessAtField = "next_process_at";
        private List<String> extFields = new ArrayList<>();

        private Builder() {
        }

        public Builder withIdField(String idField) {
            this.idField = idField;
            return this;
        }

        public Builder withQueueNameField(String queueNameField) {
            this.queueNameField = queueNameField;
            return this;
        }

        public Builder withPayloadField(String payloadField) {
            this.payloadField = payloadField;
            return this;
        }

        public Builder withAttemptField(String attemptField) {
            this.attemptField = attemptField;
            return this;
        }

        public Builder withReenqueueAttemptField(String reenqueueAttemptField) {
            this.reenqueueAttemptField = reenqueueAttemptField;
            return this;
        }

        public Builder withTotalAttemptField(String totalAttemptField) {
            this.totalAttemptField = totalAttemptField;
            return this;
        }

        public Builder withCreatedAtField(String createdAtField) {
            this.createdAtField = createdAtField;
            return this;
        }

        public Builder withNextProcessAtField(String nextProcessAtField) {
            this.nextProcessAtField = nextProcessAtField;
            return this;
        }

        public Builder withExtFields(List<String> extFields) {
            this.extFields = extFields;
            return this;
        }

        public QueueTableSchema build() {
            return new QueueTableSchema(idField, queueNameField, payloadField, attemptField, reenqueueAttemptField,
                    totalAttemptField, createdAtField, nextProcessAtField, extFields);
        }
    }
}
