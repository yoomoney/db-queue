package ru.yoomoney.tech.dbqueue.internal.pick;

import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.settings.TaskRetryType;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Настройки выборки задачи
 *
 * @author Oleg Kandaurov
 * @since 11.10.2019
 */
public class PickTaskSettings {
    @Nonnull
    private final TaskRetryType retryType;
    @Nonnull
    private final Duration retryInterval;

    public PickTaskSettings(@Nonnull TaskRetryType retryType,
                            @Nonnull Duration retryInterval) {
        this.retryType = requireNonNull(retryType);
        this.retryInterval = requireNonNull(retryInterval);
    }

    /**
     * Настройка {@link QueueSettings#getRetryType()}
     *
     * @return значение настройки
     */
    @Nonnull
    TaskRetryType getRetryType() {
        return retryType;
    }

    /**
     * Настройка {@link QueueSettings#getRetryInterval()}
     *
     * @return значение настройки
     */
    @Nonnull
    Duration getRetryInterval() {
        return retryInterval;
    }
}
