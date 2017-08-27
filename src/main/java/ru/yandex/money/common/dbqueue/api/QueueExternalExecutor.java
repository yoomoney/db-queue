package ru.yandex.money.common.dbqueue.api;

import ru.yandex.money.common.dbqueue.settings.ProcessingMode;
import ru.yandex.money.common.dbqueue.spring.SpringQueueInitializer;

import java.util.concurrent.Executor;

/**
 * Интерфейс исполнителя задач в очереди очереди.
 * <p>
 * Используется в режиме {@link ProcessingMode#USE_EXTERNAL_EXECUTOR}.
 * <p>
 * Завершением работы исполнителей управляет {@link ru.yandex.money.common.dbqueue.init.QueueRegistry}.
 * Это важно, поскольку при неправильном порядке завершения, задачи по прежнему будут братся, но
 * исполнитель не сможет их принять.
 *
 * @author Oleg Kandaurov
 * @since 30.07.2017
 */
public interface QueueExternalExecutor extends Executor {

    /**
     * Завершить работу исполнителя задач.
     * <p>
     * Данный метод необходим для того, чтобы {@link SpringQueueInitializer}
     * имел возможность завершить пулы обработки задач в правильном порядке.
     */
    void shutdownQueueExecutor();
}
