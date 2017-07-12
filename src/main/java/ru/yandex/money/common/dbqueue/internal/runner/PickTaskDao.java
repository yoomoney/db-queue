package ru.yandex.money.common.dbqueue.internal.runner;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionOperations;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static java.util.Objects.requireNonNull;

/**
 * Класс взаимодействия с БД по выборке задач
 *
 * @author Oleg Kandaurov
 * @since 15.07.2017
 */
class PickTaskDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionOperations transactionTemplate;
    private final QueueShardId shardId;

    /**
     * Конструктор
     *
     * @param shardId идентификатор шарда на котором будет происходить выборка задач
     * @param jdbcTemplate spring jdbc template
     * @param transactionTemplate spring transaction template
     */
    PickTaskDao(@Nonnull QueueShardId shardId, @Nonnull JdbcOperations jdbcTemplate,
                @Nonnull TransactionOperations transactionTemplate) {
        this.shardId = requireNonNull(shardId);
        this.jdbcTemplate = new NamedParameterJdbcTemplate(requireNonNull(jdbcTemplate));
        this.transactionTemplate = requireNonNull(transactionTemplate);
    }

    /**
     * Выбрать очередную задачу из очереди
     *
     * @param location          местоположение очереди
     * @param retryTaskStrategy стратегия повтора задачи
     * @return задача для обработки или null если таковой не нашлось
     */
    @Nullable
    TaskRecord pickTask(@Nonnull QueueLocation location, @Nonnull RetryTaskStrategy retryTaskStrategy) {
        requireNonNull(retryTaskStrategy);
        requireNonNull(location);
        MapSqlParameterSource placeholders = new MapSqlParameterSource()
                .addValue("queueName", location.getQueueName())
                .addValue("currentTime", new Timestamp(System.currentTimeMillis()));
        if (retryTaskStrategy.getPlaceholders() != null) {
            placeholders.addValues(retryTaskStrategy.getPlaceholders().getValues());
        }
        return jdbcTemplate.execute(String.format(
                "WITH cte AS (" +
                        "SELECT id " +
                        "FROM %s " +
                        "WHERE queue_name = :queueName " +
                        "  AND process_time <= :currentTime " +
                        "LIMIT 1 " +
                        "FOR UPDATE SKIP LOCKED) " +
                        "UPDATE %s q " +
                        "SET " +
                        "  process_time = %s, " +
                        "  attempt      = attempt + 1 " +
                        "FROM cte " +
                        "WHERE q.id = cte.id " +
                        "RETURNING q.id, q.task, q.attempt, q.create_time, q.process_time, " +
                        "q.log_timestamp, q.actor",
                location.getTableName(), location.getTableName(), retryTaskStrategy.getNextProcessTimeSql()),
                placeholders,
                (PreparedStatement ps) -> {
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            //noinspection ReturnOfNull
                            return null;
                        }
                        return new TaskRecord(
                                rs.getLong("id"),
                                rs.getString("task"),
                                rs.getLong("attempt"),
                                ZonedDateTime.ofInstant(rs.getTimestamp("create_time").toInstant(),
                                        ZoneId.systemDefault()),
                                ZonedDateTime.ofInstant(rs.getTimestamp("process_time").toInstant(),
                                        ZoneId.systemDefault()),
                                rs.getString("log_timestamp"),
                                rs.getString("actor"));
                    }
                });
    }

    /**
     * Получить transaction template данного шарда
     *
     * @return transaction template данного шарда
     */
    TransactionOperations getTransactionTemplate() {
        return transactionTemplate;
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
