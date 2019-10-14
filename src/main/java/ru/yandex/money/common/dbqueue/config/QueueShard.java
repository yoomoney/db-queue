package ru.yandex.money.common.dbqueue.config;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yandex.money.common.dbqueue.dao.QueueDao;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * Данные подключения к шарду БД
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
     * Конструктор
     *
     * @param databaseDialect        вид базы данных
     * @param queueTableSchema    схема таблицы очередей
     * @param shardId             идентификатор шарда
     * @param jdbcTemplate        spring jdbc template
     * @param transactionTemplate spring transaction template
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
     * Получить идентификатор данного шарда
     *
     * @return идентификатор шарда
     */
    @Nonnull
    public QueueShardId getShardId() {
        return shardId;
    }

    /**
     * Получить jdbc template данного шарда
     *
     * @return jdbc template данного шарда
     */
    @Nonnull
    public JdbcOperations getJdbcTemplate() {
        return jdbcTemplate;
    }

    /**
     * Получить transaction template данного шарда
     *
     * @return transaction template данного шарда
     */
    @Nonnull
    public TransactionOperations getTransactionTemplate() {
        return transactionTemplate;
    }

    /**
     * Возвращает объект для работы с хранилищем очереди на заданном шарде
     *
     * @return dao для работы с очередью
     */
    @Nonnull
    public QueueDao getQueueDao() {
        return queueDao;
    }

    /**
     * Возвращает вид БД для текущего шарда
     *
     * @return вид базы данных
     */
    @Nonnull
    public DatabaseDialect getDatabaseDialect() {
        return databaseDialect;
    }

    /**
     * Возвращает схема таблицы очередей для текущего шарда
     *
     * @return схема таблицы очередей
     */
    @Nonnull
    public QueueTableSchema getQueueTableSchema() {
        return queueTableSchema;
    }
}
