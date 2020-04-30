package ru.yandex.money.common.dbqueue.config;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yandex.money.common.dbqueue.dao.QueueDao;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Properties for connection to a database shard.
 *
 * @author Oleg Kandaurov
 * @since 13.08.2018
 */
public class QueueShard {

    @Nonnull
    private final DatabaseDialect databaseDialect;
    @Nonnull
    private final QueueShardId shardId;
    @Nonnull
    private final JdbcOperations jdbcTemplate;
    @Nonnull
    private final TransactionOperations transactionTemplate;
    @Nonnull
    private final QueueTableSchema queueTableSchema;
    @Nonnull
    private final QueueDao queueDao;

    /**
     * Constructor
     *
     * @param databaseDialect     Database type (dialect)
     * @param queueTableSchema    Queue table scheme.
     * @param shardId             Shard identifier.
     * @param jdbcTemplate        Reference to Spring JDBC template.
     * @param transactionTemplate Reference to Spring Transaction template.
     */
    public QueueShard(@Nonnull DatabaseDialect databaseDialect,
                      @Nonnull QueueTableSchema queueTableSchema,
                      @Nonnull QueueShardId shardId,
                      @Nonnull JdbcOperations jdbcTemplate,
                      @Nonnull TransactionOperations transactionTemplate) {
        this.databaseDialect = requireNonNull(databaseDialect);
        this.shardId = requireNonNull(shardId);
        this.jdbcTemplate = requireNonNull(jdbcTemplate);
        this.transactionTemplate = requireNonNull(transactionTemplate);
        this.queueTableSchema = requireNonNull(queueTableSchema);
        this.queueDao = QueueDao.Factory.create(databaseDialect, jdbcTemplate, queueTableSchema);
    }

    /**
     * Get shard identifier.
     *
     * @return Shard identifier.
     */
    @Nonnull
    public QueueShardId getShardId() {
        return shardId;
    }

    /**
     * Get reference to the Spring JDBC template for that shard.
     *
     * @return Reference to Spring JDBC template.
     */
    @Nonnull
    public JdbcOperations getJdbcTemplate() {
        return jdbcTemplate;
    }

    /**
     * Get reference to the Spring Transaction template for that shard.
     *
     * @return Reference to Spring Transaction template.
     */
    @Nonnull
    public TransactionOperations getTransactionTemplate() {
        return transactionTemplate;
    }

    /**
     * Get reference to database access object to work with queue storage on that shard.
     *
     * @return Reference to database access object to work with the queue.
     */
    @Nonnull
    public QueueDao getQueueDao() {
        return queueDao;
    }

    /**
     * Get database type for that shard.
     *
     * @return Database type.
     */
    @Nonnull
    public DatabaseDialect getDatabaseDialect() {
        return databaseDialect;
    }

    /**
     * Get queue table schema for that shard.
     *
     * @return Queue table schema.
     */
    @Nonnull
    public QueueTableSchema getQueueTableSchema() {
        return queueTableSchema;
    }
}
