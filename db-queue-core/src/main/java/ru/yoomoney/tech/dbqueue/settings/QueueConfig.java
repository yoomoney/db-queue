package ru.yoomoney.tech.dbqueue.settings;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Queue configuration with database table location and task processing settings.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public final class QueueConfig {

    @Nonnull
    private final QueueLocation location;
    @Nonnull
    private final QueueSettings settings;

    /**
     * Constructor for queue configuration
     *
     * @param location Queue location
     * @param settings Queue settings
     */
    public QueueConfig(@Nonnull QueueLocation location,
                       @Nonnull QueueSettings settings) {
        this.location = Objects.requireNonNull(location);
        this.settings = Objects.requireNonNull(settings);
    }

    /**
     * Get queue location.
     *
     * @return Queue location.
     */
    @Nonnull
    public QueueLocation getLocation() {
        return location;
    }

    /**
     * Get queue settings.
     *
     * @return Queue settings.
     */
    @Nonnull
    public QueueSettings getSettings() {
        return settings;
    }

    @Override
    public String toString() {
        return '{' +
                "location=" + location +
                ", settings=" + settings +
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
        QueueConfig that = (QueueConfig) obj;
        return Objects.equals(location, that.location) &&
                Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(location, settings);
    }
}
