package ru.yandex.money.common.dbqueue.internal;

import ru.yandex.money.common.dbqueue.api.Queue;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.QueueThreadLifecycleListener;
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
    private final QueueThreadLifecycleListener queueThreadLifecycleListener;

    /**
     * Конструктор
     *
     * @param loopPolicy                   стратегия выполнения цикла
     * @param queueThreadLifecycleListener слушатель событий исполнения очереди
     */
    public QueueLoop(@Nonnull LoopPolicy loopPolicy,
                     @Nonnull QueueThreadLifecycleListener queueThreadLifecycleListener) {
        this.loopPolicy = Objects.requireNonNull(loopPolicy);
        this.queueThreadLifecycleListener = Objects.requireNonNull(queueThreadLifecycleListener);
    }

    /**
     * Запустить цикл обработки задач в очерди
     *
     * @param shardId     идентификатор шарда, на котором происходит обработка
     * @param queue       выполняемая очередь
     * @param queueRunner исполнитель очереди
     */
    public void start(QueueShardId shardId, Queue queue, @Nonnull QueueRunner queueRunner) {
        Objects.requireNonNull(queueRunner);
        loopPolicy.doRun(() -> {
            try {
                queueThreadLifecycleListener.started(shardId, queue.getQueueConfig().getLocation());
                Duration waitDuration = queueRunner.runQueue(queue);
                loopPolicy.doWait(waitDuration);
            } catch (Throwable e) {
                queueThreadLifecycleListener.crashedPickTask(shardId, queue.getQueueConfig().getLocation(), e);
                loopPolicy.doWait(queue.getQueueConfig().getSettings().getFatalCrashTimeout());
            } finally {
                queueThreadLifecycleListener.finished(shardId, queue.getQueueConfig().getLocation());
            }
        });
    }

}
