package ru.yoomoney.tech.dbqueue.dao;

import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.settings.TaskRetryType;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Settings for picking up tasks via {@link QueuePickTaskDao}
 *
 * @author Oleg Kandaurov
 * @since 11.10.2019
 */
public class PickTaskSettings {
    @Nonnull
    private final TaskRetryType retryType;
    @Nonnull
    private final Duration retryInterval;

    /**
     * Constructor.
     *
     * @param retryType     setting {@link QueueSettings#getRetryType()} for a particular queue
     * @param retryInterval setting {@link QueueSettings#getRetryInterval()} for a particular queue
     */
    public PickTaskSettings(@Nonnull TaskRetryType retryType,
                            @Nonnull Duration retryInterval) {
        this.retryType = requireNonNull(retryType);
        this.retryInterval = requireNonNull(retryInterval);
    }

    /**
     * Setting {@link QueueSettings#getRetryType()}
     *
     * @return setting value
     */
    @Nonnull
    public TaskRetryType getRetryType() {
        return retryType;
    }

    /**
     * Setting {@link QueueSettings#getRetryInterval()}
     *
     * @return setting value
     */
    @Nonnull
    public Duration getRetryInterval() {
        return retryInterval;
    }
}
