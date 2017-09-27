package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.api.ThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Слушатель хода выполнения потока очереди, используемый в spring конфигурации
 *
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
public abstract class SpringThreadLifecycleListener implements ThreadLifecycleListener, SpringQueueIdentifiable {

    private final QueueId queueId;

    /**
     * Конструктор
     *
     * @param queueId идентификатор очереди
     */
    protected SpringThreadLifecycleListener(@Nonnull QueueId queueId) {
        this.queueId = Objects.requireNonNull(queueId);
    }

    @Nonnull
    @Override
    public QueueId getQueueId() {
        return queueId;
    }

}
