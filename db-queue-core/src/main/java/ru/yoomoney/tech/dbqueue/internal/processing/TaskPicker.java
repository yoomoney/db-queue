package ru.yoomoney.tech.dbqueue.internal.processing;

import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Класс, обеспечивающий выборку задачи из очереди
 *
 * @author Oleg Kandaurov
 * @since 19.07.2017
 */
@SuppressWarnings("rawtypes")
public class TaskPicker {

    @Nonnull
    private final QueueShard<?> queueShard;
    @Nonnull
    private final QueueLocation queueLocation;
    @Nonnull
    private final TaskLifecycleListener taskLifecycleListener;
    @Nonnull
    private final MillisTimeProvider millisTimeProvider;

    private final QueuePickTaskDao pickTaskDao;


    /**
     * Конструктор
     *
     * @param queueShard            шард с которого требуется выбрать задачу
     * @param taskLifecycleListener слушатель жизненного цикла задачи в очереди
     * @param millisTimeProvider    поставщик текущего времени
     * @param pickTaskDao           dao для выборки задач
     */
    public TaskPicker(@Nonnull QueueShard<?> queueShard,
                      @Nonnull QueueLocation queueLocation,
                      @Nonnull TaskLifecycleListener taskLifecycleListener,
                      @Nonnull MillisTimeProvider millisTimeProvider,
                      @Nonnull QueuePickTaskDao pickTaskDao) {
        this.queueShard = requireNonNull(queueShard);
        this.queueLocation = requireNonNull(queueLocation);
        this.taskLifecycleListener = requireNonNull(taskLifecycleListener);
        this.millisTimeProvider = requireNonNull(millisTimeProvider);
        this.pickTaskDao = requireNonNull(pickTaskDao);
    }

    /**
     * Выбрать задачу из очереди
     *
     * @return задача или null если отсутствует
     */
    @Nullable
    public TaskRecord pickTask() {
        long startPickTaskTime = millisTimeProvider.getMillis();
        TaskRecord taskRecord = queueShard.getDatabaseAccessLayer().transact(pickTaskDao::pickTask);
        if (taskRecord == null) {
            return null;
        }
        taskLifecycleListener.picked(queueShard.getShardId(), queueLocation, taskRecord,
                millisTimeProvider.getMillis() - startPickTaskTime);
        return taskRecord;
    }


}
