package ru.yoomoney.tech.dbqueue.dao;

import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.time.Duration;

/**
 * Database access object to manage tasks in the queue.
 *
 * @author Oleg Kandaurov
 * @since 06.10.2019
 */
public interface QueueDao {
    /**
     * Add a new task in the queue for processing.
     *
     * @param location      Queue location.
     * @param enqueueParams Parameters of the task
     * @return Identifier (sequence id) of new inserted task.
     */
    long enqueue(@Nonnull QueueLocation location, @Nonnull EnqueueParams<String> enqueueParams);

    /**
     * Remove (delete) task from the queue.
     *
     * @param location Queue location.
     * @param taskId   Identifier (sequence id) of the task.
     * @return true, if task was deleted from database, false, when task with given id was not found.
     */
    boolean deleteTask(@Nonnull QueueLocation location, long taskId);

    /**
     * Postpone task processing for given time period (current date and time plus execution delay).
     *
     * @param location       Queue location.
     * @param taskId         Identifier (sequence id) of the task.
     * @param executionDelay Task execution delay.
     * @return true, if task was successfully postponed, false, when task was not found.
     */
    boolean reenqueue(@Nonnull QueueLocation location, long taskId, @Nonnull Duration executionDelay);

}
