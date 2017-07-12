package ru.yandex.money.common.dbqueue.api;

import java.util.Objects;

/**
 * Хранилище данных о шарде
 *
 * @author Oleg Kandaurov
 * @since 30.07.2017
 */
public final class QueueShardId {

    private final String id;

    /**
     * Конструктор
     *
     * @param id идентификатор шарда
     */
    public QueueShardId(String id) {
        this.id = id;
    }

    /**
     * Получить идентификатор шарда
     *
     * @return идентификатор шарда
     */
    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        QueueShardId shardId = (QueueShardId) obj;
        return Objects.equals(id, shardId.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return '{' + "id=" + id + '}';
    }
}
