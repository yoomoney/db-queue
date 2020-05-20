package ru.yandex.money.common.dbqueue.internal.pick;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.config.QueueTableSchema;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.TaskRetryType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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

    private final Map<QueueLocation, String> pickTaskSqlCache = new ConcurrentHashMap<>();

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final QueueTableSchema queueTableSchema;
    private final PickTaskSettings pickTaskSettings;

    public MssqlQueuePickTaskDao(@Nonnull JdbcOperations jdbcTemplate,
                                 @Nonnull QueueTableSchema queueTableSchema,
                                 @Nonnull PickTaskSettings pickTaskSettings) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(requireNonNull(jdbcTemplate));
        this.queueTableSchema = requireNonNull(queueTableSchema);
        this.pickTaskSettings = requireNonNull(pickTaskSettings);
    }

    @Override
    @Nullable
    public TaskRecord pickTask(@Nonnull QueueLocation location) {
        requireNonNull(location);
        MapSqlParameterSource placeholders = new MapSqlParameterSource()
                .addValue("queueName", location.getQueueId().asString())
                .addValue("retryInterval", pickTaskSettings.getRetryInterval().getSeconds());

        return jdbcTemplate.execute(pickTaskSqlCache.computeIfAbsent(location, this::createPickTaskSql),
                placeholders,
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

    private String createPickTaskSql(@Nonnull QueueLocation location) {
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
                getNextProcessTimeSql(pickTaskSettings.getRetryType(), queueTableSchema) + ", " +
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
    private String getNextProcessTimeSql(@Nonnull TaskRetryType taskRetryType, QueueTableSchema queueTableSchema) {
        Objects.requireNonNull(taskRetryType);
        switch (taskRetryType) {
            case GEOMETRIC_BACKOFF:
                return "dateadd(ss, power(2, " + queueTableSchema.getAttemptField() + ") * :retryInterval, SYSDATETIMEOFFSET())";
            case ARITHMETIC_BACKOFF:
                return "dateadd(ss, (1 + (" + queueTableSchema.getAttemptField() + " * 2)) * :retryInterval, SYSDATETIMEOFFSET())";
            case LINEAR_BACKOFF:
                return "dateadd(ss, :retryInterval, SYSDATETIMEOFFSET())";
            default:
                throw new IllegalStateException("unknown retry type: " + taskRetryType);
        }
    }
}
