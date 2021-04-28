package ru.yoomoney.tech.dbqueue.stub;

import ru.yoomoney.tech.dbqueue.api.Task;
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;

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
