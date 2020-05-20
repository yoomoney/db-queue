package ru.yandex.money.common.dbqueue.dao;

import org.springframework.jdbc.core.JdbcOperations;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.config.DatabaseDialect;
import ru.yandex.money.common.dbqueue.config.QueueTableSchema;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

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

    /**
     * Factory for establishment of database-specific DAO to work with queue table.
     */
    class Factory {

        /**
         * Create a new instance of database-specific DAO based on database type (dialect).
         *
         * @param databaseDialect  Database type (dialect).
         * @param jdbcTemplate     Reference to Spring JDBC template.
         * @param queueTableSchema Queue table scheme.
         * @return New database-specific DAO instance.
         */
        public static QueueDao create(@Nonnull DatabaseDialect databaseDialect,
                                      @Nonnull JdbcOperations jdbcTemplate,
                                      @Nonnull QueueTableSchema queueTableSchema) {
            requireNonNull(databaseDialect);
            requireNonNull(jdbcTemplate);
            requireNonNull(queueTableSchema);
            switch (databaseDialect) {
                case POSTGRESQL:
                    return new PostgresQueueDao(jdbcTemplate, queueTableSchema);
                case MSSQL:
                    return new MssqlQueueDao(jdbcTemplate, queueTableSchema);
                case ORACLE_11G:
                    return new Oracle11QueueDao(jdbcTemplate, queueTableSchema);
                default:
                    throw new IllegalArgumentException("unsupported database kind: " + databaseDialect);
            }
        }
    }

}
