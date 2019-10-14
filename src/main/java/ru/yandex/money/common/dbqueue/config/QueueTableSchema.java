package ru.yandex.money.common.dbqueue.config;

import ru.yandex.money.common.dbqueue.api.TaskRecord;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Схема именования таблиц в базе данных
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

    private QueueTableSchema(@Nonnull String queueNameField,
                             @Nonnull String payloadField,
                             @Nonnull String attemptField,
                             @Nonnull String reenqueueAttemptField,
                             @Nonnull String totalAttemptField,
                             @Nonnull String createdAtField,
                             @Nonnull String nextProcessAtField,
                             @Nonnull List<String> extFields) {
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
     * Колонка в БД маппится на {@link TaskRecord#getPayload()}
     *
     * @return имя колонки
     */
    @Nonnull
    public String getPayloadField() {
        return payloadField;
    }

    /**
     * Колонка в БД маппится на {@link TaskRecord#getAttemptsCount()}
     *
     * @return имя колонки
     */
    @Nonnull
    public String getAttemptField() {
        return attemptField;
    }

    /**
     * Колонка в БД маппится на {@link TaskRecord#getReenqueueAttemptsCount()}
     *
     * @return имя колонки
     */
    @Nonnull
    public String getReenqueueAttemptField() {
        return reenqueueAttemptField;
    }

    /**
     * Колонка в БД маппится на {@link TaskRecord#getTotalAttemptsCount()}
     *
     * @return имя колонки
     */
    @Nonnull
    public String getTotalAttemptField() {
        return totalAttemptField;
    }

    /**
     * Колонка в БД маппится на {@link TaskRecord#getCreatedAt()}
     *
     * @return имя колонки
     */
    @Nonnull
    public String getCreatedAtField() {
        return createdAtField;
    }

    /**
     * Колонка с временем обработки задачи
     *
     * @return имя колонки
     */
    @Nonnull
    public String getNextProcessAtField() {
        return nextProcessAtField;
    }

    /**
     * Колонка с именем очереди
     *
     * @return имя колонки
     */
    @Nonnull
    public String getQueueNameField() {
        return queueNameField;
    }

    /**
     * Дополнительный набор текстовых колонок, маппится на {@link TaskRecord#getExtData()}
     *
     * @return имя колонок
     */
    @Nonnull
    public List<String> getExtFields() {
        return extFields;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Билдер для класса {@link QueueTableSchema}
     */
    public static class Builder {
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
            return new QueueTableSchema(queueNameField, payloadField, attemptField, reenqueueAttemptField,
                    totalAttemptField, createdAtField, nextProcessAtField, extFields);
        }
    }
}
