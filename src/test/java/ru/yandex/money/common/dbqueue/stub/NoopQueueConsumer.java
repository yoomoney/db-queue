package ru.yandex.money.common.dbqueue.stub;

import example.StringQueueConsumer;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;

/**
 * @author Oleg Kandaurov
 * @since 14.10.2019
 */
public class NoopQueueConsumer extends StringQueueConsumer {
    public NoopQueueConsumer(@Nonnull QueueConfig queueConfig) {
        super(queueConfig);
    }

    @Nonnull
    @Override
    public TaskExecutionResult execute(@Nonnull Task<String> task) {
        return TaskExecutionResult.finish();
    }
}
