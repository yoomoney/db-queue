package ru.yandex.money.common.dbqueue.config;

/**
 * Поддерживаемый вид базы данных
 *
 * @author Oleg Kandaurov
 * @since 06.10.2019
 */
public enum DatabaseDialect {
    /**
     * PostgreSQL (version equal or higher than 9.5)
     */
    POSTGRESQL,
    /**
     * MSSQL
     */
    MSSQL
}
