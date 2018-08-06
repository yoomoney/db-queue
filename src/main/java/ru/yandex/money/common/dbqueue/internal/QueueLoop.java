package ru.yandex.money.common.dbqueue.internal;

import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.ThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.internal.runner.QueueRunner;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Цикл обработки задачи в очереди.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class QueueLoop {

    @Nonnull
    private final LoopPolicy loopPolicy;
    @Nonnull
    private final ThreadLifecycleListener threadLifecycleListener;
    @Nonnull
    private final MillisTimeProvider millisTimeProvider;

    /**
     * Конструктор
     *
     * @param loopPolicy              стратегия выполнения цикла
     * @param threadLifecycleListener слушатель событий исполнения очереди
     * @param millisTimeProvider поставщик текущего времени
     */
    public QueueLoop(@Nonnull LoopPolicy loopPolicy,
                     @Nonnull ThreadLifecycleListener threadLifecycleListener,
                     @Nonnull MillisTimeProvider millisTimeProvider) {
        this.loopPolicy = Objects.requireNonNull(loopPolicy);
        this.threadLifecycleListener = Objects.requireNonNull(threadLifecycleListener);
        this.millisTimeProvider = Objects.requireNonNull(millisTimeProvider);
    }

    /**
     * Возобновить цикл обработки задач
     */
    public void wakeup() {
        loopPolicy.doContinue();
    }

    /**
     * Запустить цикл обработки задач в очерди
     *
     * @param shardId       идентификатор шарда, на котором происходит обработка
     * @param queueConsumer выполняемая очередь
     * @param queueRunner   исполнитель очереди
     */
    public void start(QueueShardId shardId, QueueConsumer queueConsumer, @Nonnull QueueRunner queueRunner) {
        Objects.requireNonNull(queueRunner);
        loopPolicy.doRun(() -> {
            try {
                long startTime = millisTimeProvider.getMillis();
                threadLifecycleListener.started(shardId, queueConsumer.getQueueConfig().getLocation());
                QueueProcessingStatus queueProcessingStatus = queueRunner.runQueue(queueConsumer);
                threadLifecycleListener.executed(shardId, queueConsumer.getQueueConfig().getLocation(),
                        queueProcessingStatus != QueueProcessingStatus.SKIPPED,
                        millisTimeProvider.getMillis() - startTime);

                switch (queueProcessingStatus) {
                    case SKIPPED:
                        loopPolicy.doWait(queueConsumer.getQueueConfig().getSettings().getNoTaskTimeout(),
                                LoopPolicy.WaitInterrupt.ALLOW);
                        return;
                    case PROCESSED:
                        loopPolicy.doWait(queueConsumer.getQueueConfig().getSettings().getBetweenTaskTimeout(),
                                LoopPolicy.WaitInterrupt.DENY);
                        return;
                    default:
                        throw new IllegalStateException("unknown task loop result" + queueProcessingStatus);
                }
            } catch (Throwable e) {
                threadLifecycleListener.crashed(shardId, queueConsumer.getQueueConfig().getLocation(), e);
                loopPolicy.doWait(queueConsumer.getQueueConfig().getSettings().getFatalCrashTimeout(),
                        LoopPolicy.WaitInterrupt.DENY);
            } finally {
                threadLifecycleListener.finished(shardId, queueConsumer.getQueueConfig().getLocation());
            }
        });
    }

}
