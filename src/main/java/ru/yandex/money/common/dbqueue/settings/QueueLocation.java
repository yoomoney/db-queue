package ru.yandex.money.common.dbqueue.settings;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Класс, определяющий местоположение очереди
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
     * Получить имя таблицы
     *
     * @return имя таблицы
     */
    @Nonnull
    public String getTableName() {
        return tableName;
    }

    /**
     * Получить идентификатор очереди
     *
     * @return идентификатор очереди
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
     * Создать билдер местоположения очереди
     *
     * @return билдер местоположения
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Билдер для класса {@link QueueLocation}
     */
    public static class Builder {
        private String tableName;
        private QueueId queueId;

        private Builder() {
        }

        /**
         * Задать имя таблицы, в которой лежат задачи очереди
         *
         * @param tableName имя таблицы
         * @return билдер
         */
        public Builder withTableName(@Nonnull String tableName) {
            this.tableName = Objects.requireNonNull(tableName);
            return this;
        }

        /**
         * Задать идентификатор очереди
         *
         * @param queueId идентификатор очереди
         * @return билдер
         */
        public Builder withQueueId(@Nonnull QueueId queueId) {
            this.queueId = Objects.requireNonNull(queueId);
            return this;
        }

        /**
         * Сконструировать местоположение очереди
         *
         * @return местоположение очереди
         */
        public QueueLocation build() {
            return new QueueLocation(queueId, tableName);
        }
    }
}
