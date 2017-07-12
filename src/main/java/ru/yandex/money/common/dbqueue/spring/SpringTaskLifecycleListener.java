package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;

/**
 * Слушатель хода обработки задачи в очереди, используемый в spring конфигурации
 *
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
public abstract class SpringTaskLifecycleListener implements TaskLifecycleListener, SpringQueueIdentifiable {

    private final QueueLocation queueLocation;

    /**
     * Конструктор
     *
     * @param queueLocation местоположение очереди
     */
    protected SpringTaskLifecycleListener(QueueLocation queueLocation) {
        this.queueLocation = queueLocation;
    }

    @Nonnull
    @Override
    public QueueLocation getQueueLocation() {
        return queueLocation;
    }

}
