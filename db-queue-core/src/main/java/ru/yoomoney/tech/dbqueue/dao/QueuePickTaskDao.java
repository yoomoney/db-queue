package ru.yoomoney.tech.dbqueue.dao;

import ru.yoomoney.tech.dbqueue.api.TaskRecord;

import javax.annotation.Nullable;

/**
 * Database access object to pick up tasks in the queue.
 *
 * @author Oleg Kandaurov
 * @since 06.10.2019
 */
public interface QueuePickTaskDao {

    /**
     * Pick task from a queue
     *
     * @return task data or null if not found
     */
    @Nullable
    TaskRecord pickTask();

}
