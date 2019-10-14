package ru.yandex.money.common.dbqueue.config;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Хранилище данных о шарде
 *
 * @author Oleg Kandaurov
 * @since 30.07.2017
 */
public final class QueueShardId {

    @Nonnull
    private final String id;

    /**
     * Конструктор
     *
     * @param id идентификатор шарда
     */
    public QueueShardId(@Nonnull String id) {
        this.id = Objects.requireNonNull(id);
    }

    /**
     * Получить идентификатор шарда
     *
     * @return идентификатор шарда
     */
    @Nonnull
    public String asString() {
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
        return id;
    }

}
