package ru.yandex.money.common.dbqueue.settings;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Класс, определяющий местоположение очереди
 *
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public class QueueLocation {
    @Nonnull
    private final String tableName;
    @Nonnull
    private final String queueName;

    /**
     * Конструктор местоположения
     *
     * @param tableName имя таблицы, в которой лежат задачи очереди
     * @param queueName имя очереди
     */
    public QueueLocation(@Nonnull String tableName, @Nonnull String queueName) {
        this.tableName = Objects.requireNonNull(tableName);
        this.queueName = Objects.requireNonNull(queueName);
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
                "table=" + tableName +
                ",queue=" + queueName +
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
}
