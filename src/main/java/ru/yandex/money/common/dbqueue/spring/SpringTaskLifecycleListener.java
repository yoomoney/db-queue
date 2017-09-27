package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;

/**
 * Слушатель хода обработки задачи в очереди, используемый в spring конфигурации
 *
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
public abstract class SpringTaskLifecycleListener implements TaskLifecycleListener, SpringQueueIdentifiable {

    private final QueueId queueId;

    /**
     * Конструктор
     *
     * @param queueId идентификатор очереди
     */
    protected SpringTaskLifecycleListener(QueueId queueId) {
        this.queueId = queueId;
    }

    @Nonnull
    @Override
    public QueueId getQueueId() {
        return queueId;
    }

}
