package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.api.ThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;

/**
 * Слушатель хода выполнения потока очереди, используемый в spring конфигурации
 *
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
public abstract class SpringThreadLifecycleListener implements ThreadLifecycleListener, SpringQueueIdentifiable {

    private final QueueLocation queueLocation;

    /**
     * Конструктор
     *
     * @param queueLocation местоположение очереди
     */
    protected SpringThreadLifecycleListener(QueueLocation queueLocation) {
        this.queueLocation = queueLocation;
    }

    @Nonnull
    @Override
    public QueueLocation getQueueLocation() {
        return queueLocation;
    }

}
