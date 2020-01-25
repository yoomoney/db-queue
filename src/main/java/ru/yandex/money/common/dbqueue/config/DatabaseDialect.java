package ru.yandex.money.common.dbqueue.config;

/**
 * Поддерживаемый вид базы данных
 *
 * @author Oleg Kandaurov
 * @since 06.10.2019
 */
public enum DatabaseDialect {
    /**
     * БД PostgreSQL выше или равной версии 9.5
     */
    POSTGRESQL,
    MSSQL
}
