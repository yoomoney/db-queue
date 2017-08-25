package ru.yandex.money.common.dbqueue.internal.runner;

import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Objects;
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

    @Nonnull
    private final TaskPicker taskPicker;
    @Nonnull
    private final TaskProcessor taskProcessor;
    @Nonnull
    private final Executor externalExecutor;

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
        this.taskPicker = Objects.requireNonNull(taskPicker);
        this.taskProcessor = Objects.requireNonNull(taskProcessor);
        this.externalExecutor = Objects.requireNonNull(externalExecutor);
    }

    @Override
    @Nonnull
    public Duration runQueue(@Nonnull QueueConsumer queueConsumer) {
        QueueConfig config = queueConsumer.getQueueConfig();
        TaskRecord taskRecord = taskPicker.pickTask(queueConsumer);
        if (taskRecord == null) {
            return config.getSettings().getNoTaskTimeout();
        }
        externalExecutor.execute(() -> taskProcessor.processTask(queueConsumer, taskRecord));
        return config.getSettings().getBetweenTaskTimeout();
    }

}
