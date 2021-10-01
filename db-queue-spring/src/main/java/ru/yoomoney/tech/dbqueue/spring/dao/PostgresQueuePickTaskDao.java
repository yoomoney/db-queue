package ru.yoomoney.tech.dbqueue.spring.dao;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;
import ru.yoomoney.tech.dbqueue.settings.FailRetryType;
import ru.yoomoney.tech.dbqueue.settings.FailureSettings;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Database access object to pick tasks in the queue for PostgreSQL database type.
 *
 * @author Oleg Kandaurov
 * @since 15.07.2017
 */
public class PostgresQueuePickTaskDao implements QueuePickTaskDao {

    private String pickTaskSql;
    private MapSqlParameterSource pickTaskSqlPlaceholders;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final QueueTableSchema queueTableSchema;

    /**
     * Конструктор
     *
     * @param jdbcTemplate     spring jdbc template
     * @param queueTableSchema схема таблицы очередей
     */
    public PostgresQueuePickTaskDao(@Nonnull JdbcOperations jdbcTemplate,
                                    @Nonnull QueueTableSchema queueTableSchema,
                                    @Nonnull QueueLocation queueLocation,
                                    @Nonnull FailureSettings failureSettings) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(requireNonNull(jdbcTemplate));
        this.queueTableSchema = requireNonNull(queueTableSchema);
        this.pickTaskSql = createPickTaskSql(queueLocation, failureSettings);
        pickTaskSqlPlaceholders = new MapSqlParameterSource()
                .addValue("queueName", queueLocation.getQueueId().asString())
                .addValue("retryInterval", failureSettings.getRetryInterval().getSeconds());
        failureSettings.registerObserver((oldValue, newValue) -> {
            pickTaskSql = createPickTaskSql(queueLocation, newValue);
            pickTaskSqlPlaceholders = new MapSqlParameterSource()
                    .addValue("queueName", queueLocation.getQueueId().asString())
                    .addValue("retryInterval", newValue.getRetryInterval().getSeconds());
        });
    }

    @Override
    @Nullable
    public TaskRecord pickTask() {
        return jdbcTemplate.execute(pickTaskSql, pickTaskSqlPlaceholders,
                (PreparedStatement ps) -> {
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            //noinspection ReturnOfNull
                            return null;
                        }

                        Map<String, String> additionalData = new LinkedHashMap<>();
                        queueTableSchema.getExtFields().forEach(key -> {
                            try {
                                additionalData.put(key, rs.getString(key));
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        });
                        return TaskRecord.builder()
                                .withId(rs.getLong(queueTableSchema.getIdField()))
                                .withCreatedAt(getZonedDateTime(rs, queueTableSchema.getCreatedAtField()))
                                .withNextProcessAt(getZonedDateTime(rs, queueTableSchema.getNextProcessAtField()))
                                .withPayload(rs.getString(queueTableSchema.getPayloadField()))
                                .withAttemptsCount(rs.getLong(queueTableSchema.getAttemptField()))
                                .withReenqueueAttemptsCount(rs.getLong(queueTableSchema.getReenqueueAttemptField()))
                                .withTotalAttemptsCount(rs.getLong(queueTableSchema.getTotalAttemptField()))
                                .withExtData(additionalData).build();
                    }
                });
    }

    private String createPickTaskSql(@Nonnull QueueLocation location, @Nonnull FailureSettings failureSettings) {
        return "WITH cte AS (" +
                "SELECT " + queueTableSchema.getIdField() + " " +
                "FROM " + location.getTableName() + " " +
                "WHERE " + queueTableSchema.getQueueNameField() + " = :queueName " +
                "  AND " + queueTableSchema.getNextProcessAtField() + " <= now() " +
                " ORDER BY " + queueTableSchema.getNextProcessAtField() + " ASC " +
                "LIMIT 1 " +
                "FOR UPDATE SKIP LOCKED) " +
                "UPDATE " + location.getTableName() + " q " +
                "SET " +
                "  " + queueTableSchema.getNextProcessAtField() + " = " +
                getNextProcessTimeSql(failureSettings.getRetryType(), queueTableSchema) + ", " +
                "  " + queueTableSchema.getAttemptField() + " = " + queueTableSchema.getAttemptField() + " + 1, " +
                "  " + queueTableSchema.getTotalAttemptField() + " = " + queueTableSchema.getTotalAttemptField() + " + 1 " +
                "FROM cte " +
                "WHERE q." + queueTableSchema.getIdField() + " = cte." + queueTableSchema.getIdField() + " " +
                "RETURNING q." + queueTableSchema.getIdField() + ", " +
                "q." + queueTableSchema.getPayloadField() + ", " +
                "q." + queueTableSchema.getAttemptField() + ", " +
                "q." + queueTableSchema.getReenqueueAttemptField() + ", " +
                "q." + queueTableSchema.getTotalAttemptField() + ", " +
                "q." + queueTableSchema.getCreatedAtField() + ", " +
                "q." + queueTableSchema.getNextProcessAtField() +
                (queueTableSchema.getExtFields().isEmpty() ? "" : queueTableSchema.getExtFields().stream()
                        .map(field -> "q." + field).collect(Collectors.joining(", ", ", ", "")));
    }

    private ZonedDateTime getZonedDateTime(ResultSet rs, String time) throws SQLException {
        return ZonedDateTime.ofInstant(rs.getTimestamp(time).toInstant(), ZoneId.systemDefault());
    }


    @Nonnull
    private String getNextProcessTimeSql(@Nonnull FailRetryType failRetryType, QueueTableSchema queueTableSchema) {
        requireNonNull(failRetryType);
        switch (failRetryType) {
            case GEOMETRIC_BACKOFF:
                return "now() + power(2, " + queueTableSchema.getAttemptField() + ") * :retryInterval * INTERVAL '1 SECOND'";
            case ARITHMETIC_BACKOFF:
                return "now() + (1 + (" + queueTableSchema.getAttemptField() + " * 2)) * :retryInterval * INTERVAL '1 SECOND'";
            case LINEAR_BACKOFF:
                return "now() + :retryInterval * INTERVAL '1 SECOND'";
            default:
                throw new IllegalStateException("unknown retry type: " + failRetryType);
        }
    }
}
