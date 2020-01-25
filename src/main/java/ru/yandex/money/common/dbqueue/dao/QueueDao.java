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
 * Dao для управления задачами в очереди
 *
 * @author Oleg Kandaurov
 * @since 06.10.2019
 */
public interface QueueDao {
    /**
     * Поставить задачу в очередь на выполнение
     *
     * @param location      местоположение очереди
     * @param enqueueParams данные вставляемой задачи
     * @return идентфикатор (sequence id) вставленной задачи
     */
    long enqueue(@Nonnull QueueLocation location, @Nonnull EnqueueParams<String> enqueueParams);

    /**
     * Удалить задачу из очереди.
     *
     * @param location местоположение очеред
     * @param taskId   идентификатор (sequence id) задачи
     * @return true, если задача была удалена, false, если задача не найдена.
     */
    boolean deleteTask(@Nonnull QueueLocation location, long taskId);

    /**
     * Переставить выполнение задачи на заданный промежуток времени
     *
     * @param location       местоположение очереди
     * @param taskId         идентификатор (sequence id) задачи
     * @param executionDelay промежуток времени
     * @return true, если задача была переставлена, false, если задача не найдена.
     */
    boolean reenqueue(@Nonnull QueueLocation location, long taskId, @Nonnull Duration executionDelay);

    /**
     * Фабрика для создания БД-специфичных DAO для работы с таблицей очередей
     */
    class Factory {

        /**
         * Создать инстанс dao для работы с данными очередями в зависимости от вида БД
         *
         * @param databaseDialect     вид базы данных
         * @param jdbcTemplate     spring jdbc template
         * @param queueTableSchema схема таблицы очередей
         * @return dao для работы с очередями
         */
        public static QueueDao create(@Nonnull DatabaseDialect databaseDialect,
                                      @Nonnull JdbcOperations jdbcTemplate,
                                      @Nonnull QueueTableSchema queueTableSchema) {
            requireNonNull(databaseDialect);
            requireNonNull(jdbcTemplate);
            requireNonNull(queueTableSchema);
            //noinspection SwitchStatementWithTooFewBranches
            switch (databaseDialect) {
                case POSTGRESQL:
                    return new PostgresQueueDao(jdbcTemplate, queueTableSchema);
                case MSSQL:
                    return new MssqlQueueDao(jdbcTemplate, queueTableSchema);
                default:
                    throw new IllegalArgumentException("unsupported database kind: " + databaseDialect);
            }
        }
    }

}
