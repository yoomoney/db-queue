package ru.yoomoney.tech.dbqueue.settings;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Queue identifier.
 *
 * @author Oleg Kandaurov
 * @since 27.09.2017
 */
public final class QueueId {

    @Nonnull
    private final String id;

    /**
     * Constructor
     *
     * @param id String representation of queue identifier.
     */
    public QueueId(@Nonnull String id) {
        this.id = Objects.requireNonNull(id, "queue id must not be null");
    }

    /**
     * Get string representation of queue identifier.
     *
     * @return Queue identifier.
     */
    @Nonnull
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
