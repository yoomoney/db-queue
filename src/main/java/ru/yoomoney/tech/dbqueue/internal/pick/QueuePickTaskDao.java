package ru.yoomoney.tech.dbqueue.internal.pick;

import org.springframework.jdbc.core.JdbcOperations;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Класс взаимодействия с БД для выборки задач
 *
 * @author Oleg Kandaurov
 * @since 06.10.2019
 */
public interface QueuePickTaskDao {

    /**
     * Выбрать очередную задачу из очереди
     *
     * @param location местоположение очереди
     * @return задача для обработки или null если таковой не нашлось
     */
    @Nullable
    TaskRecord pickTask(@Nonnull QueueLocation location);

    /**
     * Фабрика для создания БД-специфичных DAO для выборки очередей
     */
    class Factory {

        /**
         * Создать инстанс dao для выборки задач из очереди в зависимости от вида БД
         *
         * @param databaseDialect     вид базы данных
         * @param jdbcTemplate     spring jdbc template
         * @param queueTableSchema схема таблицы очередей
         * @param pickTaskSettings настройки выборки задач
         * @return dao для работы с очередями
         */
        public static QueuePickTaskDao create(@Nonnull DatabaseDialect databaseDialect,
                                              @Nonnull QueueTableSchema queueTableSchema,
                                              @Nonnull JdbcOperations jdbcTemplate,
                                              @Nonnull PickTaskSettings pickTaskSettings) {
            requireNonNull(databaseDialect);
            requireNonNull(queueTableSchema);
            requireNonNull(jdbcTemplate);
            requireNonNull(pickTaskSettings);
            switch (databaseDialect) {
                case POSTGRESQL:
                    return new PostgresQueuePickTaskDao(jdbcTemplate, queueTableSchema, pickTaskSettings);
                case MSSQL:
                    return new MssqlQueuePickTaskDao(jdbcTemplate, queueTableSchema, pickTaskSettings);
                case ORACLE_11G:
                    return new Oracle11QueuePickTaskDao(jdbcTemplate, queueTableSchema, pickTaskSettings);
                default:
                    throw new IllegalArgumentException("unsupported database kind: " + databaseDialect);
            }
        }
    }
}
