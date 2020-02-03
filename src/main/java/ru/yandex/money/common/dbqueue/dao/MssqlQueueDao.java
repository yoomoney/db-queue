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
 * Dao для управления задачами в очереди БД MsSQL.
 *
 * @author Oleg Kandaurov
 * @author Behrooz Shabani
 * @since 25.01.2020
 */
public class MssqlQueueDao implements QueueDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final String enqueueSql;
    private final String deleteTaskSql;
    private final String reenqueueTaskSql;
    @Nonnull
    private final QueueTableSchema queueTableSchema;

    /**
     * constructor
     *
     * @param jdbcTemplate     spring jdbc template
     * @param queueTableSchema definition of queue table
     */
    public MssqlQueueDao(@Nonnull JdbcOperations jdbcTemplate,
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
                ") OUTPUT inserted.id VALUES " +
                "(:queueName, :payload, dateadd(ss, :executionDelay, SYSDATETIMEOFFSET()), 0, 0" +
                (queueTableSchema.getExtFields().isEmpty() ? "" : queueTableSchema.getExtFields().stream()
                        .map(field -> ":" + field).collect(Collectors.joining(", ", ", ", ""))) +
                ")";

        deleteTaskSql = "DELETE FROM %s WHERE " + queueTableSchema.getQueueNameField() +
                " = :queueName AND id = :id";

        reenqueueTaskSql = "UPDATE %s SET " + queueTableSchema.getNextProcessAtField() +
                " = dateadd(ss, :executionDelay, SYSDATETIMEOFFSET()), " +
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
