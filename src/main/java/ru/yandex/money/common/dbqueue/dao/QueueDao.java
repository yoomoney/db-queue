package ru.yandex.money.common.dbqueue.dao;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionOperations;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Управление задачами в очереди.
 *
 * Каждый экземпляр данного класса привязан к физическому шарду БД.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public class QueueDao {

    private final QueueShardId shardId;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionOperations transactionTemplate;

    /**
     * Конструктор.
     * <p>
     * Клиентский код обязан обеспечить уникальную привязку пары (jdbcTemplate, transactionTemplate) к shardId
     *
     * @param shardId             идентификатор шарда
     * @param jdbcTemplate        spring jdbc template
     * @param transactionTemplate spring transaction template
     */
    public QueueDao(QueueShardId shardId, JdbcOperations jdbcTemplate, TransactionOperations transactionTemplate) {
        this.shardId = shardId;
        this.jdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        this.transactionTemplate = transactionTemplate;
    }

    /**
     * Поставить задачу в очередь на выполнение
     * @param location местоположение очереди
     * @param enqueueParams данные вставляемой задачи
     * @return идентфикатор (sequence id) вставленной задачи
     */
    public long enqueue(@Nonnull QueueLocation location, @Nonnull EnqueueParams<String> enqueueParams) {
        requireNonNull(location);
        requireNonNull(enqueueParams);
        return jdbcTemplate.queryForObject(
                String.format("INSERT INTO %s(queue_name, task, process_time, log_timestamp, actor) VALUES " +
                                "(:queueName, :task, now() + :executionDelay * INTERVAL '1 SECOND', " +
                                ":correlationId, :actor) RETURNING id",
                        location.getTableName()),
                new MapSqlParameterSource()
                        .addValue("queueName", location.getQueueName())
                        .addValue("task", enqueueParams.getPayload())
                        .addValue("executionDelay", enqueueParams.getExecutionDelay().getSeconds())
                        .addValue("correlationId", enqueueParams.getCorrelationId())
                        .addValue("actor", enqueueParams.getActor()),
                Long.class);
    }

    /**
     * Удалить задачу из очереди.
     *
     * @param location местоположение очеред
     * @param taskId идентификатор (sequence id) задачи
     * @return true, если задача была удалена, false, если задача не найдена.
     */
    public boolean deleteTask(@Nonnull QueueLocation location, long taskId) {
        requireNonNull(location);
        int updatedRows = jdbcTemplate.update(String.format("DELETE FROM %s " +
                        "WHERE queue_name = :queueName AND id = :id", location.getTableName()),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueName()));
        return updatedRows != 0;
    }

    /**
     * Переставить выполнение задачи на заданный промежуток времени
     *
     * @param location местоположение очереди
     * @param taskId идентификатор (sequence id) задами
     * @param executionDelay промежуток времени
     * @param resetAttempts признак, что следует сбросить попытки выполнения задачи
     * @return true, если задача была переставлена, false, если задача не найдена.
     */
    public boolean reenqueue(@Nonnull QueueLocation location, long taskId, @Nonnull Duration executionDelay,
                             boolean resetAttempts) {
        requireNonNull(location);
        requireNonNull(executionDelay);
        int updatedRows = jdbcTemplate.update(String.format("UPDATE %s " +
                        "SET" +
                        "  process_time = now() + :executionDelay * INTERVAL '1 SECOND'" +
                        (resetAttempts ? ",  attempt = 0 " : "") +
                        "WHERE id = :id AND queue_name = :queueName", location.getTableName()),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueName())
                        .addValue("executionDelay", executionDelay.getSeconds()));
        return updatedRows != 0;
    }

    /**
     * Получить transaction template данного шарда
     *
     * @return transaction template данного шарда
     */
    public TransactionOperations getTransactionTemplate() {
        return transactionTemplate;
    }

    /**
     * Получить jdbc template данного шарда
     *
     * @return jdbc template данного шарда
     */
    public JdbcOperations getJdbcTemplate() {
        return jdbcTemplate.getJdbcOperations();
    }

    /**
     * Получить идентификатор данного шарда
     *
     * @return идентификатор шарда
     */
    public QueueShardId getShardId() {
        return shardId;
    }
}
