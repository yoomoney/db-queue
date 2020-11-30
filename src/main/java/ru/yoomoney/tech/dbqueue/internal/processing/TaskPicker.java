package ru.yoomoney.tech.dbqueue.internal.processing;

import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.internal.pick.PickTaskSettings;
import ru.yoomoney.tech.dbqueue.internal.pick.QueuePickTaskDao;

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
    private final QueueShard queueShard;
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
     * @param pickTaskSettings      настройки выборки задачи
     */
    public TaskPicker(QueueShard queueShard,
                      TaskLifecycleListener taskLifecycleListener,
                      MillisTimeProvider millisTimeProvider,
                      PickTaskSettings pickTaskSettings) {
        this(queueShard, taskLifecycleListener, millisTimeProvider,
                QueuePickTaskDao.Factory.create(queueShard.getDatabaseDialect(),
                        queueShard.getQueueTableSchema(), queueShard.getJdbcTemplate(), pickTaskSettings));
    }

    /**
     * Конструктор
     *
     * @param queueShard            шард с которого требуется выбрать задачу
     * @param taskLifecycleListener слушатель жизненного цикла задачи в очереди
     * @param millisTimeProvider    поставщик текущего времени
     * @param pickTaskDao           dao для выборки задач
     */
    TaskPicker(@Nonnull QueueShard queueShard,
               @Nonnull TaskLifecycleListener taskLifecycleListener,
               @Nonnull MillisTimeProvider millisTimeProvider,
               @Nonnull QueuePickTaskDao pickTaskDao) {
        this.queueShard = requireNonNull(queueShard);
        this.taskLifecycleListener = requireNonNull(taskLifecycleListener);
        this.millisTimeProvider = requireNonNull(millisTimeProvider);
        this.pickTaskDao = requireNonNull(pickTaskDao);
    }

    /**
     * Выбрать задачу из очереди
     *
     * @param queueConsumer очередь для выборки
     * @return задача или null если отсуствует
     */
    @Nullable
    public TaskRecord pickTask(@Nonnull QueueConsumer queueConsumer) {
        requireNonNull(queueConsumer);
        long startPickTaskTime = millisTimeProvider.getMillis();
        TaskRecord taskRecord = queueShard.getTransactionTemplate()
                .execute(status -> pickTaskDao.pickTask(queueConsumer.getQueueConfig().getLocation()));
        if (taskRecord == null) {
            return null;
        }
        taskLifecycleListener.picked(queueShard.getShardId(), queueConsumer.getQueueConfig().getLocation(),
                taskRecord, millisTimeProvider.getMillis() - startPickTaskTime);
        return taskRecord;
    }


}
