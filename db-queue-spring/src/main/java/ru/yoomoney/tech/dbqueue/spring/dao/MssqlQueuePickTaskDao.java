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
 * Database access object to manage tasks in the queue for Microsoft SQL server database type.
 *
 * @author Oleg Kandaurov
 * @author Behrooz Shabani
 * @since 25.01.2020
 */
public class MssqlQueuePickTaskDao implements QueuePickTaskDao {

    private String pickTaskSql;
    private MapSqlParameterSource pickTaskSqlPlaceholders;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final QueueTableSchema queueTableSchema;

    public MssqlQueuePickTaskDao(@Nonnull JdbcOperations jdbcTemplate,
                                 @Nonnull QueueTableSchema queueTableSchema,
                                 @Nonnull QueueLocation queueLocation,
                                 @Nonnull FailureSettings failureSettings) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(requireNonNull(jdbcTemplate));
        this.queueTableSchema = requireNonNull(queueTableSchema);
        pickTaskSqlPlaceholders = new MapSqlParameterSource()
                .addValue("queueName", queueLocation.getQueueId().asString())
                .addValue("retryInterval", failureSettings.getRetryInterval().getSeconds());
        pickTaskSql = createPickTaskSql(queueLocation, failureSettings);
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
        return jdbcTemplate.execute(pickTaskSql,
                pickTaskSqlPlaceholders,
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

    private String createPickTaskSql(@Nonnull QueueLocation location, FailureSettings failureSettings) {
        return "WITH cte AS (" +
                "SELECT " + queueTableSchema.getIdField() + " " +
                "FROM " + location.getTableName() + " with (readpast, updlock) " +
                "WHERE " + queueTableSchema.getQueueNameField() + " = :queueName " +
                "  AND " + queueTableSchema.getNextProcessAtField() + " <= SYSDATETIMEOFFSET() " +
                " ORDER BY " + queueTableSchema.getNextProcessAtField() + " ASC " +
                "offset 0 rows fetch next 1 rows only " +
                ") " +
                "UPDATE " + location.getTableName() + " " +
                "SET " +
                "  " + queueTableSchema.getNextProcessAtField() + " = " +
                getNextProcessTimeSql(failureSettings.getRetryType(), queueTableSchema) + ", " +
                "  " + queueTableSchema.getAttemptField() + " = " + queueTableSchema.getAttemptField() + " + 1, " +
                "  " + queueTableSchema.getTotalAttemptField() + " = " + queueTableSchema.getTotalAttemptField() + " + 1 " +
                "OUTPUT inserted." + queueTableSchema.getIdField() + ", " +
                "inserted." + queueTableSchema.getPayloadField() + ", " +
                "inserted." + queueTableSchema.getAttemptField() + ", " +
                "inserted." + queueTableSchema.getReenqueueAttemptField() + ", " +
                "inserted." + queueTableSchema.getTotalAttemptField() + ", " +
                "inserted." + queueTableSchema.getCreatedAtField() + ", " +
                "inserted." + queueTableSchema.getNextProcessAtField() +
                (queueTableSchema.getExtFields().isEmpty() ? "" : queueTableSchema.getExtFields().stream()
                        .map(field -> "inserted." + field).collect(Collectors.joining(", ", ", ", ""))) + " " +
                "FROM cte " +
                "WHERE " + location.getTableName() + "." + queueTableSchema.getIdField() + " = cte." + queueTableSchema.getIdField();
    }

    private ZonedDateTime getZonedDateTime(ResultSet rs, String time) throws SQLException {
        return ZonedDateTime.ofInstant(rs.getTimestamp(time).toInstant(), ZoneId.systemDefault());
    }


    @Nonnull
    private String getNextProcessTimeSql(@Nonnull FailRetryType failRetryType, QueueTableSchema queueTableSchema) {
        requireNonNull(failRetryType);
        switch (failRetryType) {
            case GEOMETRIC_BACKOFF:
                return "dateadd(ss, power(2, " + queueTableSchema.getAttemptField() + ") * :retryInterval, SYSDATETIMEOFFSET())";
            case ARITHMETIC_BACKOFF:
                return "dateadd(ss, (1 + (" + queueTableSchema.getAttemptField() + " * 2)) * :retryInterval, SYSDATETIMEOFFSET())";
            case LINEAR_BACKOFF:
                return "dateadd(ss, :retryInterval, SYSDATETIMEOFFSET())";
            default:
                throw new IllegalStateException("unknown retry type: " + failRetryType);
        }
    }
}
