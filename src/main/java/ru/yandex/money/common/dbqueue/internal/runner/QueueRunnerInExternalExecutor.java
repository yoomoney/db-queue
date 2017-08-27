package ru.yandex.money.common.dbqueue.internal.runner;

import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.internal.QueueProcessingStatus;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;

import javax.annotation.Nonnull;
import java.util.concurrent.Executor;

/**
 * Исполнитель задач очереди в режиме
 * {@link ProcessingMode#USE_EXTERNAL_EXECUTOR}
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class QueueRunnerInExternalExecutor implements QueueRunner {

    private final BaseQueueRunner baseQueueRunner;

    /**
     * Конструктор
     *
     * @param taskPicker       выборщик задачи
     * @param taskProcessor    обработчик задачи
     * @param externalExecutor исполнитель задачи
     */
    QueueRunnerInExternalExecutor(@Nonnull TaskPicker taskPicker,
                                  @Nonnull TaskProcessor taskProcessor,
                                  @Nonnull Executor externalExecutor) {
        baseQueueRunner = new BaseQueueRunner(taskPicker, taskProcessor, externalExecutor);
    }

    @Override
    @Nonnull
    public QueueProcessingStatus runQueue(@Nonnull QueueConsumer queueConsumer) {
        return baseQueueRunner.runQueue(queueConsumer);
    }

}
