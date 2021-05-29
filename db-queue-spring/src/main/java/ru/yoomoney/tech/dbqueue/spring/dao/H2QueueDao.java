package ru.yoomoney.tech.dbqueue.spring.dao;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.dao.QueueDao;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class H2QueueDao implements QueueDao {
    private final Map<QueueLocation, String> enqueueSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> deleteSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> reenqueueSqlCache = new ConcurrentHashMap<>();

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final QueueTableSchema queueTableSchema;

    public H2QueueDao(@Nonnull final JdbcOperations jdbcOperations,
                      @Nonnull final QueueTableSchema queueTableSchema) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(requireNonNull(jdbcOperations, "jdbc template can't be null"));
        this.queueTableSchema = Objects.requireNonNull(queueTableSchema, "table schema can't be null");
    }

    @Override
    public long enqueue(@Nonnull final QueueLocation location,
                        @Nonnull final EnqueueParams<String> enqueueParams) {
        requireNonNull(location, "location can't be null");
        requireNonNull(enqueueParams, "params can't be null");

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("queueName", location.getQueueId().asString())
                .addValue("payload", enqueueParams.getPayload())
                .addValue("executionDelay", enqueueParams.getExecutionDelay().getSeconds());

        queueTableSchema
                .getExtFields()
                .forEach(paramName -> params.addValue(paramName, null));
        enqueueParams
                .getExtData()
                .forEach(params::addValue);

        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(
                enqueueSqlCache.computeIfAbsent(location, this::createEnqueueSql),
                params, keyHolder, new String[] {queueTableSchema.getIdField()});

        return requireNonNull(keyHolder.getKeyAs(Long.class));
    }

    @Override
    public boolean deleteTask(@Nonnull final QueueLocation location, final long taskId) {
        requireNonNull(location, "location can't be null");

        int updatedRows = jdbcTemplate.update(
                deleteSqlCache.computeIfAbsent(location, this::createDeleteSql),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString()));
        return updatedRows != 0;
    }

    @Override
    public boolean reenqueue(@Nonnull final QueueLocation location,
                             final long taskId,
                             @Nonnull final Duration executionDelay) {
        requireNonNull(location, "location can't be null");
        requireNonNull(executionDelay, "delay can't be null");

        int updatedRows = jdbcTemplate.update(
                reenqueueSqlCache.computeIfAbsent(location, this::createReenqueueSql),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString())
                        .addValue("executionDelay", executionDelay.getSeconds()));
        return updatedRows != 0;
    }

    private String createEnqueueSql(final @Nonnull QueueLocation location) {
        return String.format("" +
                        "INSERT INTO %s (" +
                        "   %s " +
                        "   %s, " +
                        "   %s, " +
                        "   %s, " +
                        "   %s, " +
                        "   %s " +
                        "   %s" +
                        ") VALUES (" +
                        "   %s " +
                        "   :queueName, " +
                        "   :payload, " +
                        "   TIMESTAMPADD(SECOND, :executionDelay , NOW()), " +
                        "   0, " +
                        "   0 " +
                        "   %s " +
                        ")",
                location.getTableName(),
                location.getIdSequence()
                        .map(x -> queueTableSchema.getIdField())
                        .map(field -> field + ",")
                        .orElse(""),
                queueTableSchema.getQueueNameField(),
                queueTableSchema.getPayloadField(),
                queueTableSchema.getNextProcessAtField(),
                queueTableSchema.getReenqueueAttemptField(),
                queueTableSchema.getTotalAttemptField(),
                queueTableSchema.getExtFields().isEmpty()
                        ? "" :
                        queueTableSchema
                                .getExtFields()
                                .stream()
                                .collect(Collectors.joining(", ", ", ", "")),

                location.getIdSequence()
                        .map(seq -> String.format(" NEXTVAL('%s'), ", seq))
                        .orElse(""),

                queueTableSchema.getExtFields().isEmpty()
                        ? "" :
                        queueTableSchema
                                .getExtFields()
                                .stream()
                                .map(field -> ":" + field)
                                .collect(Collectors.joining(", ", ", ", ""))
        );
    }

    private String createDeleteSql(@Nonnull QueueLocation location) {
        return String.format("DELETE FROM %s WHERE %s = :queueName AND %s = :id",
                location.getTableName(),
                queueTableSchema.getQueueNameField(),
                queueTableSchema.getIdField());
    }

    private String createReenqueueSql(final QueueLocation location) {
        return String.format("" +
                        "UPDATE %s " +
                        "SET " +
                        "   %s = TIMESTAMPADD(SECOND, :executionDelay , NOW()), " +
                        "   %s = 0, " +
                        "   %s = %s + 1 " +
                        "WHERE %s = :id AND %s = :queueName",

                location.getTableName(),
                queueTableSchema.getNextProcessAtField(),
                queueTableSchema.getAttemptField(),
                queueTableSchema.getReenqueueAttemptField(),
                queueTableSchema.getReenqueueAttemptField(),
                queueTableSchema.getIdField(),
                queueTableSchema.getQueueNameField()
        );
    }
}
