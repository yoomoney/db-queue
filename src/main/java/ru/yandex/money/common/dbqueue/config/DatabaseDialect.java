package ru.yandex.money.common.dbqueue.config;

/**
 * Supported database type (dialect)
 *
 * @author Oleg Kandaurov
 * @since 06.10.2019
 */
public enum DatabaseDialect {
    /**
     * PostgreSQL (version equals or higher than 9.5).
     */
    POSTGRESQL,
    /**
     * Microsoft SQL Server
     */
    MSSQL,
    /**
     * Oracle 11g
     *
     * This version doesn't have automatically incremented primary keys,
     * so you must specify sequence name in
     * {@link ru.yandex.money.common.dbqueue.settings.QueueLocation.Builder#withIdSequence(String)}
     */
    ORACLE_11G
}
