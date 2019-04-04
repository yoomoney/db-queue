package ru.yandex.money.common.dbqueue.api;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yandex.money.common.dbqueue.dao.QueueDao;

import javax.annotation.Nonnull;

/**
 * Данные подключения к шарду БД
 *
 * @author Oleg Kandaurov
 * @since 13.08.2018
 */
public class QueueShard {

    private final QueueShardId shardId;
    private final JdbcOperations jdbcTemplate;
    private final TransactionOperations transactionTemplate;
    private final QueueDao queueDao;

    /**
     * Конструктор
     *
     * @param shardId             идентификатор шарда
     * @param jdbcTemplate        spring jdbc template
     * @param transactionTemplate spring transaction template
     */
    public QueueShard(@Nonnull QueueShardId shardId,
                      @Nonnull JdbcOperations jdbcTemplate,
                      @Nonnull TransactionOperations transactionTemplate) {
        this.shardId = shardId;
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.queueDao = new QueueDao(jdbcTemplate);
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
     * Получить инстанс класса для работы с задачями в очереди на текущем шарде
     *
     * @return класс для работы с очередью
     */
    @Nonnull
    public QueueDao getQueueDao() {
        return queueDao;
    }
}
