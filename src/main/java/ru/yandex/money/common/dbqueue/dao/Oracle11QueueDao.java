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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Database access object to manage tasks in the queue for Oracle database type.
 *
 * @author Oleg Kandaurov
 * @since 15.05.2020
 */
public class Oracle11QueueDao implements QueueDao {

    private final Map<QueueLocation, String> enqueueSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> deleteSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> reenqueueSqlCache = new ConcurrentHashMap<>();
    private final Map<String, String> nextSequenceSqlCache = new ConcurrentHashMap<>();

    @Nonnull
    private final NamedParameterJdbcTemplate jdbcTemplate;
    @Nonnull
    private final QueueTableSchema queueTableSchema;

    /**
     * Constructor
     *
     * @param jdbcTemplate     Reference to Spring JDBC template.
     * @param queueTableSchema Queue table scheme.
     */
    public Oracle11QueueDao(@Nonnull JdbcOperations jdbcTemplate,
                            @Nonnull QueueTableSchema queueTableSchema) {
        this.queueTableSchema = requireNonNull(queueTableSchema);
        this.jdbcTemplate = new NamedParameterJdbcTemplate(requireNonNull(jdbcTemplate));
    }

    @Override
    @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE", "SQL_INJECTION_SPRING_JDBC"})
    public long enqueue(@Nonnull QueueLocation location, @Nonnull EnqueueParams<String> enqueueParams) {
        requireNonNull(location);
        requireNonNull(enqueueParams);

        String idSequence = location.getIdSequence()
                .orElseThrow(() -> new IllegalStateException("id sequence must be specified for oracle 11g database"));

        Long generatedId = Objects.requireNonNull(jdbcTemplate.getJdbcTemplate().queryForObject(
                nextSequenceSqlCache.computeIfAbsent(idSequence, this::createNextSequenceSql), Long.class));

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("queueName", location.getQueueId().asString())
                .addValue("payload", enqueueParams.getPayload())
                .addValue("executionDelay", enqueueParams.getExecutionDelay().getSeconds())
                .addValue("id", generatedId);

        queueTableSchema.getExtFields().forEach(paramName -> params.addValue(paramName, null));
        enqueueParams.getExtData().forEach(params::addValue);

        jdbcTemplate.update(enqueueSqlCache.computeIfAbsent(location, this::createEnqueueSql), params);
        return generatedId;
    }


    @Override
    public boolean deleteTask(@Nonnull QueueLocation location, long taskId) {
        requireNonNull(location);

        int updatedRows = jdbcTemplate.update(deleteSqlCache.computeIfAbsent(location, this::createDeleteSql),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString()));
        return updatedRows != 0;
    }

    @Override
    public boolean reenqueue(@Nonnull QueueLocation location, long taskId, @Nonnull Duration executionDelay) {
        requireNonNull(location);
        requireNonNull(executionDelay);
        int updatedRows = jdbcTemplate.update(reenqueueSqlCache.computeIfAbsent(location, this::createReenqueueSql),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString())
                        .addValue("executionDelay", executionDelay.getSeconds()));
        return updatedRows != 0;
    }

    private String createDeleteSql(@Nonnull QueueLocation location) {
        return "DELETE FROM " + location.getTableName() + " WHERE " + queueTableSchema.getQueueNameField() +
                " = :queueName AND " + queueTableSchema.getIdField() + " = :id";
    }

    private String createEnqueueSql(@Nonnull QueueLocation location) {
        return "INSERT INTO " + location.getTableName() + "(" +
                queueTableSchema.getIdField() + "," +
                queueTableSchema.getQueueNameField() + "," +
                queueTableSchema.getPayloadField() + "," +
                queueTableSchema.getNextProcessAtField() + "," +
                queueTableSchema.getReenqueueAttemptField() + "," +
                queueTableSchema.getTotalAttemptField() +
                (queueTableSchema.getExtFields().isEmpty() ? "" :
                        queueTableSchema.getExtFields().stream().collect(Collectors.joining(", ", ", ", ""))) +
                ") VALUES " +
                "(:id, :queueName, :payload, CURRENT_TIMESTAMP + :executionDelay * INTERVAL '1' SECOND, 0, 0" +
                (queueTableSchema.getExtFields().isEmpty() ? "" : queueTableSchema.getExtFields().stream()
                        .map(field -> ":" + field).collect(Collectors.joining(", ", ", ", ""))) +
                ")";
    }

    private String createReenqueueSql(@Nonnull QueueLocation location) {
        return "UPDATE " + location.getTableName() + " SET " + queueTableSchema.getNextProcessAtField() +
                " = CURRENT_TIMESTAMP + :executionDelay * INTERVAL '1' SECOND, " +
                queueTableSchema.getAttemptField() + " = 0, " +
                queueTableSchema.getReenqueueAttemptField() +
                " = " + queueTableSchema.getReenqueueAttemptField() + " + 1 " +
                "WHERE " + queueTableSchema.getIdField() + " = :id AND " +
                queueTableSchema.getQueueNameField() + " = :queueName";
    }

    private String createNextSequenceSql(String idSequence) {
        return "SELECT " + idSequence + ".nextval FROM dual";
    }

}
