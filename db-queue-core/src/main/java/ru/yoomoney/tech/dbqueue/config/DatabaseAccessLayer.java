package ru.yoomoney.tech.dbqueue.config;

import ru.yoomoney.tech.dbqueue.dao.PickTaskSettings;
import ru.yoomoney.tech.dbqueue.dao.QueueDao;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

/**
 * Interface for interacting with database
 *
 * @author Oleg Kandaurov
 * @since 22.04.2021
 */
public interface DatabaseAccessLayer {

    /**
     * Get an instance of database-specific DAO based on database type and table schema.
     *
     * @return database-specific DAO instance.
     */
    QueueDao getQueueDao();

    /**
     * Create an instance of database-specific DAO based on database type and table schema.
     *
     * @param pickTaskSettings settings for picking up tasks
     * @return database-specific DAO instance.
     */
    QueuePickTaskDao createQueuePickTaskDao(@Nonnull PickTaskSettings pickTaskSettings);

    /**
     * Perform an operation in transaction
     *
     * @param supplier operation
     * @param <T>      result type
     * @return result of operation
     */
    <T> T transact(Supplier<T> supplier);

    /**
     * Perform an operation in transaction
     *
     * @param runnable operation
     */
    void transact(Runnable runnable);

    /**
     * Get database type for that database.
     *
     * @return Database type.
     */
    @Nonnull
    DatabaseDialect getDatabaseDialect();

    /**
     * Get queue table schema for that database.
     *
     * @return Queue table schema.
     */
    @Nonnull
    QueueTableSchema getQueueTableSchema();

}
