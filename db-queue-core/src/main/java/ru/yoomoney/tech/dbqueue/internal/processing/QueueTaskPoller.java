package ru.yoomoney.tech.dbqueue.internal.processing;

import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.config.ThreadLifecycleListener;
import ru.yoomoney.tech.dbqueue.internal.runner.QueueRunner;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Цикл обработки задачи в очереди.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class QueueTaskPoller {

    @Nonnull
    private final ThreadLifecycleListener threadLifecycleListener;
    @Nonnull
    private final MillisTimeProvider millisTimeProvider;

    /**
     * Конструктор
     *
     * @param threadLifecycleListener слушатель событий исполнения очереди
     * @param millisTimeProvider      поставщик текущего времени
     */
    public QueueTaskPoller(@Nonnull ThreadLifecycleListener threadLifecycleListener,
                           @Nonnull MillisTimeProvider millisTimeProvider) {
        this.threadLifecycleListener = requireNonNull(threadLifecycleListener);
        this.millisTimeProvider = requireNonNull(millisTimeProvider);
    }

    /**
     * Запустить цикл обработки задач в очереди
     *
     * @param queueLoop     стратегия выполнения цикла
     * @param shardId       идентификатор шарда, на котором происходит обработка
     * @param queueConsumer выполняемая очередь
     * @param queueRunner   исполнитель очереди
     */
    public void start(@Nonnull QueueLoop queueLoop,
                      @Nonnull QueueShardId shardId,
                      @Nonnull QueueConsumer queueConsumer,
                      @Nonnull QueueRunner queueRunner) {
        requireNonNull(shardId);
        requireNonNull(queueConsumer);
        requireNonNull(queueRunner);
        requireNonNull(queueLoop);
        queueLoop.doRun(() -> {
            try {
                long startTime = millisTimeProvider.getMillis();
                threadLifecycleListener.started(shardId, queueConsumer.getQueueConfig().getLocation());
                QueueProcessingStatus queueProcessingStatus = queueRunner.runQueue(queueConsumer);
                threadLifecycleListener.executed(shardId, queueConsumer.getQueueConfig().getLocation(),
                        queueProcessingStatus != QueueProcessingStatus.SKIPPED,
                        millisTimeProvider.getMillis() - startTime);

                switch (queueProcessingStatus) {
                    case SKIPPED:
                        queueLoop.doWait(queueConsumer.getQueueConfig().getSettings().getNoTaskTimeout(),
                                QueueLoop.WaitInterrupt.ALLOW);
                        return;
                    case PROCESSED:
                        queueLoop.doWait(queueConsumer.getQueueConfig().getSettings().getBetweenTaskTimeout(),
                                QueueLoop.WaitInterrupt.DENY);
                        return;
                    default:
                        throw new IllegalStateException("unknown task loop result" + queueProcessingStatus);
                }
            } catch (Throwable e) {
                threadLifecycleListener.crashed(shardId, queueConsumer.getQueueConfig().getLocation(), e);
                queueLoop.doWait(queueConsumer.getQueueConfig().getSettings().getFatalCrashTimeout(),
                        QueueLoop.WaitInterrupt.DENY);
            } finally {
                threadLifecycleListener.finished(shardId, queueConsumer.getQueueConfig().getLocation());
            }
        });
    }

}
