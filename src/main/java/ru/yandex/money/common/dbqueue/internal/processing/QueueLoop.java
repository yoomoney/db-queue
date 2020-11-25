package ru.yandex.money.common.dbqueue.internal.processing;

import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.config.QueueShardId;
import ru.yandex.money.common.dbqueue.config.ThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.internal.runner.QueueRunner;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

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
     * @param millisTimeProvider      поставщик текущего времени
     */
    public QueueLoop(@Nonnull LoopPolicy loopPolicy,
                     @Nonnull ThreadLifecycleListener threadLifecycleListener,
                     @Nonnull MillisTimeProvider millisTimeProvider) {
        this.loopPolicy = requireNonNull(loopPolicy);
        this.threadLifecycleListener = requireNonNull(threadLifecycleListener);
        this.millisTimeProvider = requireNonNull(millisTimeProvider);
    }

    /**
     * Возобновить цикл обработки задач
     */
    public void wakeup() {
        loopPolicy.doContinue();
    }

    /**
     * Запустить цикл обработки задач в очереди
     *
     * @param shardId       идентификатор шарда, на котором происходит обработка
     * @param queueConsumer выполняемая очередь
     * @param queueRunner   исполнитель очереди
     */
    public void start(@Nonnull QueueShardId shardId,
                      @Nonnull QueueConsumer queueConsumer,
                      @Nonnull QueueRunner queueRunner) {
        requireNonNull(shardId);
        requireNonNull(queueConsumer);
        requireNonNull(queueRunner);
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
            } catch (Exception e) {
                threadLifecycleListener.crashed(shardId, queueConsumer.getQueueConfig().getLocation(), e);
                loopPolicy.doWait(queueConsumer.getQueueConfig().getSettings().getFatalCrashTimeout(),
                        LoopPolicy.WaitInterrupt.DENY);
            } finally {
                threadLifecycleListener.finished(shardId, queueConsumer.getQueueConfig().getLocation());
            }
        });
    }

    /**
     * Прекратить работу цикла обработки задач в очереди
     */
    public void pause() {
        loopPolicy.pause();
    }

    /**
     * Возобновить работу цикла обработки задач в очереди
     */
    public void unpause() {
        loopPolicy.unpause();
    }

    /**
     * Возвращает признак, что выполнение приостановлено
     *
     * @return признак приостановки выполнения
     */
    public boolean isPaused() {
        return loopPolicy.isPaused();
    }

}
