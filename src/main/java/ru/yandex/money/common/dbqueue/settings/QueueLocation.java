package ru.yandex.money.common.dbqueue.settings;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Класс, определяющий местоположение очереди
 *
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public final class QueueLocation {
    @Nonnull
    private final String tableName;
    @Nonnull
    private final String queueName;

    private QueueLocation(@Nonnull String queueName, @Nonnull String tableName) {
        this.queueName = Objects.requireNonNull(queueName);
        this.tableName = Objects.requireNonNull(tableName);
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
     * Получить название очереди
     *
     * @return название очереди
     */
    @Nonnull
    public String getQueueName() {
        return queueName;
    }

    @Override
    public String toString() {
        return '{' +
                "queue=" + queueName +
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
                Objects.equals(queueName, that.queueName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, queueName);
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
        private String queueName;

        private Builder(){
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
         * Задать имя очереди
         *
         * @param queueName имя очереди
         * @return билдер
         */
        public Builder withQueueName(@Nonnull String queueName) {
            this.queueName = Objects.requireNonNull(queueName);
            return this;
        }

        /**
         * Сконструировать местоположение очереди
         *
         * @return местоположение очереди
         */
        public QueueLocation build() {
            return new QueueLocation(queueName, tableName);
        }
    }
}
