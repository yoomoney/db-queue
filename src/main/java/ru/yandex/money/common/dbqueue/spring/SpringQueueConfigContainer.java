package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Хранилище настроек всех очередей
 *
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
public class SpringQueueConfigContainer {

    @Nonnull
    private final Map<QueueLocation, QueueConfig> queueConfigs;

    /**
     * Конструктор
     *
     * @param queueConfigs список настроек очередей
     */
    public SpringQueueConfigContainer(@Nonnull Collection<QueueConfig> queueConfigs) {
        Objects.requireNonNull(queueConfigs);
        this.queueConfigs = queueConfigs.stream()
                .collect(Collectors.toMap(QueueConfig::getLocation, Function.identity()));
    }

    /**
     * Получить конфигурацию требуемой очереди
     * @param queueLocation местоположение очереди
     * @return настройки очереди
     */
    @Nonnull
    public Optional<QueueConfig> getQueueConfig(@Nonnull QueueLocation queueLocation) {
        Objects.requireNonNull(queueLocation);
        return Optional.ofNullable(queueConfigs.get(queueLocation));
    }

    /**
     * Получить конфигурацию всех очередей
     * @return Map: key - местоположение очереди, value - конфигурация данной очереди
     */
    @Nonnull
    public Map<QueueLocation, QueueConfig> getQueueConfigs() {
        return Collections.unmodifiableMap(queueConfigs);
    }
}
