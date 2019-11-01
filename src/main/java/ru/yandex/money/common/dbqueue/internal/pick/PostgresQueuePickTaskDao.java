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
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Реализация класса взаимодействия с PostgreSQL для выборки задач
 *
 * @author Oleg Kandaurov
 * @since 15.07.2017
 */
public class PostgresQueuePickTaskDao implements QueuePickTaskDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final String pickTaskSql;
    private final QueueTableSchema queueTableSchema;
    private final PickTaskSettings pickTaskSettings;
    private final String nextProcessTimeSql;

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
        this.nextProcessTimeSql = getNextProcessTimeSql(pickTaskSettings.getRetryType(), queueTableSchema);
        pickTaskSql = "WITH cte AS (" +
                "SELECT id " +
                "FROM %s " +
                "WHERE " + queueTableSchema.getQueueNameField() + " = :queueName " +
                "  AND " + queueTableSchema.getNextProcessAtField() + " <= now() " +
                " ORDER BY " + queueTableSchema.getNextProcessAtField() + " ASC, id DESC " +
                "LIMIT 1 " +
                "FOR UPDATE SKIP LOCKED) " +
                "UPDATE %s q " +
                "SET " +
                "  " + queueTableSchema.getNextProcessAtField() + " = %s, " +
                "  " + queueTableSchema.getAttemptField() + " = " + queueTableSchema.getAttemptField() + " + 1, " +
                "  " + queueTableSchema.getTotalAttemptField() + " = coalesce(" + queueTableSchema.getTotalAttemptField() + ", 0) + 1 " +
                "FROM cte " +
                "WHERE q.id = cte.id " +
                "RETURNING q.id, " +
                "q." + queueTableSchema.getPayloadField() + ", " +
                "q." + queueTableSchema.getAttemptField() + ", " +
                "q." + queueTableSchema.getReenqueueAttemptField() + ", " +
                "q." + queueTableSchema.getTotalAttemptField() + ", " +
                "q." + queueTableSchema.getCreatedAtField() + ", " +
                "q." + queueTableSchema.getNextProcessAtField() +
                (queueTableSchema.getExtFields().isEmpty() ? "" : queueTableSchema.getExtFields().stream()
                        .map(field -> "q." + field).collect(Collectors.joining(", ", ", ", "")));
    }

    @Override
    @Nullable
    public TaskRecord pickTask(@Nonnull QueueLocation location) {
        requireNonNull(location);
        MapSqlParameterSource placeholders = new MapSqlParameterSource()
                .addValue("queueName", location.getQueueId().asString())
                .addValue("retryInterval", pickTaskSettings.getRetryInterval().toString());

        return jdbcTemplate.execute(String.format(
                pickTaskSql,
                location.getTableName(), location.getTableName(),
                nextProcessTimeSql),
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
                                .withId(rs.getLong("id"))
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

    private ZonedDateTime getZonedDateTime(ResultSet rs, String time) throws SQLException {
        return ZonedDateTime.ofInstant(rs.getTimestamp(time).toInstant(), ZoneId.systemDefault());
    }


    @Nonnull
    private String getNextProcessTimeSql(@Nonnull TaskRetryType taskRetryType, QueueTableSchema queueTableSchema) {
        Objects.requireNonNull(taskRetryType);
        switch (taskRetryType) {
            case GEOMETRIC_BACKOFF:
                return "now() + power(2, " + queueTableSchema.getAttemptField() + ") * :retryInterval :: interval";
            case ARITHMETIC_BACKOFF:
                return "now() + (1 + (" + queueTableSchema.getAttemptField() + " * 2)) * :retryInterval :: interval";
            case LINEAR_BACKOFF:
                return "now() + :retryInterval :: interval";
            default:
                throw new IllegalStateException("unknown retry type: " + taskRetryType);
        }
    }
}
