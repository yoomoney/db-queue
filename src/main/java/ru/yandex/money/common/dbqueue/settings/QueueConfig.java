package ru.yandex.money.common.dbqueue.settings;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Конфигурация очереди
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public class QueueConfig {

    @Nonnull
    private final QueueLocation location;
    @Nonnull
    private final QueueSettings settings;

    /**
     * Конструктор конфигурации очереди
     *
     * @param location местоположение очереди
     * @param settings настройки очереди
     */
    public QueueConfig(@Nonnull QueueLocation location,
                       @Nonnull QueueSettings settings) {
        this.location = Objects.requireNonNull(location);
        this.settings = Objects.requireNonNull(settings);
    }

    /**
     * Получить местоположение очереди
     *
     * @return местоположение очереди
     */
    @Nonnull
    public QueueLocation getLocation() {
        return location;
    }

    /**
     * Получить настройки очереди
     *
     * @return настройки очереди
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
}
