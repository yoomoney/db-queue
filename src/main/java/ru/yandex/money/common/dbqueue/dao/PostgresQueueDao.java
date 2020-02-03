package ru.yandex.money.common.dbqueue.dao;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.config.QueueTableSchema;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Dao для управления задачами в очереди БД PostgreSQL.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public class PostgresQueueDao implements QueueDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final String enqueueSql;
    private final String deleteTaskSql;
    private final String reenqueueTaskSql;
    @Nonnull
    private final QueueTableSchema queueTableSchema;

    /**
     * Конструктор
     *
     * @param jdbcTemplate     spring jdbc template
     * @param queueTableSchema схема таблицы очередей
     */
    public PostgresQueueDao(@Nonnull JdbcOperations jdbcTemplate,
                            @Nonnull QueueTableSchema queueTableSchema) {
        requireNonNull(jdbcTemplate);

        this.queueTableSchema = requireNonNull(queueTableSchema);
        this.jdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        enqueueSql = "INSERT INTO %s(" +
                queueTableSchema.getQueueNameField() + "," +
                queueTableSchema.getPayloadField() + "," +
                queueTableSchema.getNextProcessAtField() + "," +
                queueTableSchema.getReenqueueAttemptField() + "," +
                queueTableSchema.getTotalAttemptField() +
                (queueTableSchema.getExtFields().isEmpty() ? "" :
                        queueTableSchema.getExtFields().stream().collect(Collectors.joining(", ", ", ", ""))) +
                ") VALUES " +
                "(:queueName, :payload, now() + :executionDelay * INTERVAL '1 SECOND', 0, 0" +
                (queueTableSchema.getExtFields().isEmpty() ? "" : queueTableSchema.getExtFields().stream()
                        .map(field -> ":" + field).collect(Collectors.joining(", ", ", ", ""))) +
                ") RETURNING id";

        deleteTaskSql = "DELETE FROM %s WHERE " + queueTableSchema.getQueueNameField() +
                " = :queueName AND id = :id";

        reenqueueTaskSql = "UPDATE %s SET " + queueTableSchema.getNextProcessAtField() +
                " = now() + :executionDelay * INTERVAL '1 SECOND', " +
                queueTableSchema.getAttemptField() + " = 0, " +
                queueTableSchema.getReenqueueAttemptField() +
                " = " + queueTableSchema.getReenqueueAttemptField() + " + 1 " +
                "WHERE id = :id AND " +
                queueTableSchema.getQueueNameField() + " = :queueName";
    }

    @Override
    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public long enqueue(@Nonnull QueueLocation location, @Nonnull EnqueueParams<String> enqueueParams) {
        requireNonNull(location);
        requireNonNull(enqueueParams);

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("queueName", location.getQueueId().asString())
                .addValue("payload", enqueueParams.getPayload())
                .addValue("executionDelay", enqueueParams.getExecutionDelay().getSeconds());

        queueTableSchema.getExtFields().forEach(paramName -> params.addValue(paramName, null));
        enqueueParams.getExtData().forEach(params::addValue);
        return requireNonNull(jdbcTemplate.queryForObject(
                String.format(enqueueSql, location.getTableName()), params, Long.class));
    }


    @Override
    public boolean deleteTask(@Nonnull QueueLocation location, long taskId) {
        requireNonNull(location);

        int updatedRows = jdbcTemplate.update(String.format(deleteTaskSql, location.getTableName()),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString()));
        return updatedRows != 0;
    }

    @Override
    public boolean reenqueue(@Nonnull QueueLocation location, long taskId, @Nonnull Duration executionDelay) {
        requireNonNull(location);
        requireNonNull(executionDelay);
        int updatedRows = jdbcTemplate.update(String.format(reenqueueTaskSql, location.getTableName()),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString())
                        .addValue("executionDelay", executionDelay.getSeconds()));
        return updatedRows != 0;
    }

}
