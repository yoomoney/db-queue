package ru.yandex.money.common.dbqueue.internal;

import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.ThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.internal.runner.QueueRunner;

import javax.annotation.Nonnull;
import java.time.Duration;
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

    /**
     * Конструктор
     *
     * @param loopPolicy              стратегия выполнения цикла
     * @param threadLifecycleListener слушатель событий исполнения очереди
     */
    public QueueLoop(@Nonnull LoopPolicy loopPolicy,
                     @Nonnull ThreadLifecycleListener threadLifecycleListener) {
        this.loopPolicy = Objects.requireNonNull(loopPolicy);
        this.threadLifecycleListener = Objects.requireNonNull(threadLifecycleListener);
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
                threadLifecycleListener.started(shardId, queueConsumer.getQueueConfig().getLocation());
                Duration waitDuration = queueRunner.runQueue(queueConsumer);
                loopPolicy.doWait(waitDuration);
            } catch (Throwable e) {
                threadLifecycleListener.crashed(shardId, queueConsumer.getQueueConfig().getLocation(), e);
                loopPolicy.doWait(queueConsumer.getQueueConfig().getSettings().getFatalCrashTimeout());
            } finally {
                threadLifecycleListener.finished(shardId, queueConsumer.getQueueConfig().getLocation());
            }
        });
    }

}
