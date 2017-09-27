package ru.yandex.money.common.dbqueue.settings;

import java.util.Objects;

/**
 * Идентификатор очереди
 *
 * @author Oleg Kandaurov
 * @since 27.09.2017
 */
public final class QueueId {

    private final String id;

    /**
     * Конструктор
     *
     * @param id строковое представление идентификатора
     */
    public QueueId(String id) {
        this.id = id;
    }

    /**
     * Получить строковое представление идентификатора
     *
     * @return идентификатор очереди
     */
    public String asString() {
        return id;
    }

    @Override
    public String toString() {
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
        QueueId queueId = (QueueId) obj;
        return Objects.equals(id, queueId.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
