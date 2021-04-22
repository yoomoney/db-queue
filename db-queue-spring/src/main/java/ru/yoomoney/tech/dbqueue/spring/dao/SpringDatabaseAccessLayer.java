package ru.yoomoney.tech.dbqueue.spring.dao;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.dao.PickTaskSettings;
import ru.yoomoney.tech.dbqueue.dao.QueueDao;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Class for interacting with database via Spring JDBC
 *
 * @author Oleg Kandaurov
 * @since 22.04.2021
 */
public class SpringDatabaseAccessLayer implements DatabaseAccessLayer {

    private final JdbcOperations jdbcOperations;
    private final TransactionOperations transactionOperations;
    private final DatabaseDialect databaseDialect;
    private final QueueTableSchema queueTableSchema;
    private final QueueDao queueDao;


    /**
     * Constructor
     *
     * @param databaseDialect       Database type (dialect)
     * @param queueTableSchema      Queue table scheme.
     * @param jdbcOperations        Reference to Spring JDBC template.
     * @param transactionOperations Reference to Spring transaction template.
     */
    public SpringDatabaseAccessLayer(@Nonnull DatabaseDialect databaseDialect,
                                     @Nonnull QueueTableSchema queueTableSchema,
                                     @Nonnull JdbcOperations jdbcOperations,
                                     @Nonnull TransactionOperations transactionOperations) {
        this.databaseDialect = requireNonNull(databaseDialect);
        this.queueTableSchema = requireNonNull(queueTableSchema);
        this.jdbcOperations = requireNonNull(jdbcOperations);
        this.transactionOperations = requireNonNull(transactionOperations);
        this.queueDao = createQueueDao(databaseDialect, queueTableSchema, jdbcOperations);
    }

    @Override
    public QueueDao getQueueDao() {
        return queueDao;
    }

    private QueueDao createQueueDao(@Nonnull DatabaseDialect databaseDialect,
                                    @Nonnull QueueTableSchema queueTableSchema,
                                    @Nonnull JdbcOperations jdbcOperations) {
        requireNonNull(databaseDialect);
        requireNonNull(jdbcOperations);
        requireNonNull(queueTableSchema);
        switch (databaseDialect) {
            case POSTGRESQL:
                return new PostgresQueueDao(jdbcOperations, queueTableSchema);
            case MSSQL:
                return new MssqlQueueDao(jdbcOperations, queueTableSchema);
            case ORACLE_11G:
                return new Oracle11QueueDao(jdbcOperations, queueTableSchema);
            default:
                throw new IllegalArgumentException("unsupported database kind: " + databaseDialect);
        }
    }

    @Override
    public QueuePickTaskDao createQueuePickTaskDao(@Nonnull PickTaskSettings pickTaskSettings) {
        requireNonNull(databaseDialect);
        requireNonNull(queueTableSchema);
        requireNonNull(pickTaskSettings);
        switch (databaseDialect) {
            case POSTGRESQL:
                return new PostgresQueuePickTaskDao(jdbcOperations, queueTableSchema, pickTaskSettings);
            case MSSQL:
                return new MssqlQueuePickTaskDao(jdbcOperations, queueTableSchema, pickTaskSettings);
            case ORACLE_11G:
                return new Oracle11QueuePickTaskDao(jdbcOperations, queueTableSchema, pickTaskSettings);
            default:
                throw new IllegalArgumentException("unsupported database kind: " + databaseDialect);
        }
    }

    @Nonnull
    @Override
    public DatabaseDialect getDatabaseDialect() {
        return databaseDialect;
    }

    @Nonnull
    @Override
    public QueueTableSchema getQueueTableSchema() {
        return queueTableSchema;
    }

    @Override
    public <T> T transact(Supplier<T> supplier) {
        return transactionOperations.execute((status) -> supplier.get());
    }

    @Override
    public void transact(Runnable runnable) {
        transact(() -> {
            runnable.run();
            return null;
        });
    }


    /**
     * Get reference to Spring JDBC template.
     *
     * @return Reference to Spring JDBC template.
     */
    public JdbcOperations getJdbcOperations() {
        return jdbcOperations;
    }

    /**
     * Get reference to Spring transaction template.
     *
     * @return Reference to Spring transaction template.
     */
    public TransactionOperations getTransactionOperations() {
        return transactionOperations;
    }
}
