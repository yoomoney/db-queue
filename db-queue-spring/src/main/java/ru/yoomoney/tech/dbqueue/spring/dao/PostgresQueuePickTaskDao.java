package ru.yoomoney.tech.dbqueue.spring.dao;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.dao.PickTaskSettings;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.TaskRetryType;

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
 * Database access object to pick tasks in the queue for PostgreSQL database type.
 *
 * @author Oleg Kandaurov
 * @since 15.07.2017
 */
public class PostgresQueuePickTaskDao implements QueuePickTaskDao {

    private final Map<QueueLocation, String> pickTaskSqlCache = new ConcurrentHashMap<>();

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final QueueTableSchema queueTableSchema;
    private final PickTaskSettings pickTaskSettings;

    /**
     * Конструктор
     *
     * @param jdbcTemplate     spring jdbc template
     * @param queueTableSchema схема таблицы очередей
     * @param pickTaskSettings настройки выборки задач
     */
    public PostgresQueuePickTaskDao(@Nonnull JdbcOperations jdbcTemplate,
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
                "FROM " + location.getTableName() + " " +
                "WHERE " + queueTableSchema.getQueueNameField() + " = :queueName " +
                "  AND " + queueTableSchema.getNextProcessAtField() + " <= now() " +
                " ORDER BY " + queueTableSchema.getNextProcessAtField() + " ASC " +
                "LIMIT 1 " +
                "FOR UPDATE SKIP LOCKED) " +
                "UPDATE " + location.getTableName() + " q " +
                "SET " +
                "  " + queueTableSchema.getNextProcessAtField() + " = " +
                getNextProcessTimeSql(pickTaskSettings.getRetryType(), queueTableSchema) + ", " +
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
    private String getNextProcessTimeSql(@Nonnull TaskRetryType taskRetryType, QueueTableSchema queueTableSchema) {
        Objects.requireNonNull(taskRetryType);
        switch (taskRetryType) {
            case GEOMETRIC_BACKOFF:
                return "now() + power(2, " + queueTableSchema.getAttemptField() + ") * :retryInterval * INTERVAL '1 SECOND'";
            case ARITHMETIC_BACKOFF:
                return "now() + (1 + (" + queueTableSchema.getAttemptField() + " * 2)) * :retryInterval * INTERVAL '1 SECOND'";
            case LINEAR_BACKOFF:
                return "now() + :retryInterval * INTERVAL '1 SECOND'";
            default:
                throw new IllegalStateException("unknown retry type: " + taskRetryType);
        }
    }
}
