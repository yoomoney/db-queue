package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;

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
    private final Map<QueueId, QueueConfig> queueConfigs;

    /**
     * Конструктор
     *
     * @param queueConfigs список настроек очередей
     */
    public SpringQueueConfigContainer(@Nonnull Collection<QueueConfig> queueConfigs) {
        Objects.requireNonNull(queueConfigs);
        this.queueConfigs = queueConfigs.stream()
                .collect(Collectors.toMap(conf -> conf.getLocation().getQueueId(), Function.identity()));
    }

    /**
     * Получить конфигурацию требуемой очереди
     *
     * @param queueId идентификатор очереди
     * @return настройки очереди
     */
    @Nonnull
    public Optional<QueueConfig> getQueueConfig(@Nonnull QueueId queueId) {
        Objects.requireNonNull(queueId);
        return Optional.ofNullable(queueConfigs.get(queueId));
    }

    /**
     * Получить конфигурацию всех очередей
     *
     * @return Map: key - идентификатор очереди, value - конфигурация данной очереди
     */
    @Nonnull
    public Map<QueueId, QueueConfig> getQueueConfigs() {
        return Collections.unmodifiableMap(queueConfigs);
    }

    @Override
    public String toString() {
        return '{' +
                "queueConfigs=" + queueConfigs +
                '}';
    }
}
