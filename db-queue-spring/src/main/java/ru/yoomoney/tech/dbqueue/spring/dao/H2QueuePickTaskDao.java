package ru.yoomoney.tech.dbqueue.spring.dao;

import org.springframework.dao.support.DataAccessUtils;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Database access object to manage tasks in the queue for H2 database type.
 */
public class H2QueuePickTaskDao implements QueuePickTaskDao {
    private final RowIdLocker rowIdLocker = new RowIdLocker();

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final QueueTableSchema queueTableSchema;
    private final PickTaskSettings pickTaskSettings;

    public H2QueuePickTaskDao(@Nonnull final JdbcOperations jdbcOperations,
                              @Nonnull final QueueTableSchema queueTableSchema,
                              @Nonnull final PickTaskSettings pickTaskSettings) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(Objects.requireNonNull(jdbcOperations, "jdbc template can't be null"));
        this.queueTableSchema = Objects.requireNonNull(queueTableSchema, "table schema can't be null");
        this.pickTaskSettings = Objects.requireNonNull(pickTaskSettings, "settings can't be null");
    }

    @Nullable
    @Override
    public TaskRecord pickTask(@Nonnull final QueueLocation location) {
        Objects.requireNonNull(location, "location can't be null");

        final long retryInterval = pickTaskSettings.getRetryInterval().getSeconds();
        final String queueId = location.getQueueId().asString();

        final Long taskId = rowIdLocker.lock(
                queueId,
                rowIds -> {
                    List<Long> ids = jdbcTemplate
                            .queryForList(
                                    getSelectSql(location, queueTableSchema),
                                    new MapSqlParameterSource()
                                            .addValue("queueId", queueId)
                                            .addValue("rowIds", rowIds),
                                    Long.class);
                    return DataAccessUtils.singleResult(ids);
                });
        if (taskId == null) {
            return null;
        }

        try {
            int updatedRowCount = jdbcTemplate.update(
                    getUpdateSql(location, pickTaskSettings, queueTableSchema),
                    new MapSqlParameterSource()
                            .addValue("retryInterval", retryInterval)
                            .addValue("taskId", taskId));

            if (updatedRowCount != 1) {
                throw new IllegalStateException("Something wrong went here. Only row must be updated, not more!");
            }


            return jdbcTemplate.query(
                    getReturnSql(location, queueTableSchema),
                    new MapSqlParameterSource("taskId", taskId),
                    (ResultSet rs) -> {
                        if (!rs.next()) {
                            return null;
                        }

                        Map<String, String> additionalData = queueTableSchema
                                .getExtFields()
                                .stream()
                                .collect(
                                        LinkedHashMap::new,
                                        (map, key) -> {
                                            try {
                                                map.put(key, rs.getString(key));
                                            } catch (SQLException e) {
                                                throw new RuntimeException(e);
                                            }
                                        }, Map::putAll);

                        return TaskRecord.builder()
                                .withId(rs.getLong(queueTableSchema.getIdField()))
                                .withCreatedAt(getZonedDateTime(rs, queueTableSchema.getCreatedAtField()))
                                .withNextProcessAt(getZonedDateTime(rs, queueTableSchema.getNextProcessAtField()))
                                .withPayload(rs.getString(queueTableSchema.getPayloadField()))
                                .withAttemptsCount(rs.getLong(queueTableSchema.getAttemptField()))
                                .withReenqueueAttemptsCount(rs.getLong(queueTableSchema.getReenqueueAttemptField()))
                                .withTotalAttemptsCount(rs.getLong(queueTableSchema.getTotalAttemptField()))
                                .withExtData(additionalData).build();

                    });
        } finally {
            rowIdLocker.unlock(queueId, taskId);
        }
    }

    private static String getSelectSql(final QueueLocation location,
                                       final QueueTableSchema queueTableSchema) {
        return String.format("" +
                        "SELECT %s " +
                        "FROM %s " +
                        "WHERE %s = :queueId " +
                        "  AND %s <= now() " +
                        "  AND _ROWID_ NOT IN (:rowIds) " +
                        "ORDER BY %s ASC " +
                        "LIMIT 1 ",
                queueTableSchema.getIdField(),
                location.getTableName(),
                queueTableSchema.getQueueNameField(),
                queueTableSchema.getNextProcessAtField(),
                queueTableSchema.getNextProcessAtField()
        );
    }

    private static String getUpdateSql(final QueueLocation location,
                                       final PickTaskSettings pickTaskSettings,
                                       final QueueTableSchema queueTableSchema) {
        return String.format("" +
                        "UPDATE %s " +
                        "SET " +
                        "  %s = %s, " +
                        "  %s = %s + 1, " +
                        "  %s = %s + 1 " +
                        "WHERE %s = :taskId ",
                location.getTableName(),
                queueTableSchema.getNextProcessAtField(),
                getNextProcessTimeSql(pickTaskSettings.getRetryType(), queueTableSchema),
                queueTableSchema.getAttemptField(),
                queueTableSchema.getAttemptField(),
                queueTableSchema.getTotalAttemptField(),
                queueTableSchema.getTotalAttemptField(),
                queueTableSchema.getIdField());
    }

    private static String getReturnSql(final QueueLocation location,
                                       final QueueTableSchema queueTableSchema) {
        return String.format("" +
                        "SELECT  " +
                        "   %s, " +
                        "   %s, " +
                        "   %s, " +
                        "   %s, " +
                        "   %s, " +
                        "   %s, " +
                        "   %s  " +
                        "   %s  " +
                        "FROM %s " +
                        "WHERE %s = :taskId",
                queueTableSchema.getIdField(),
                queueTableSchema.getPayloadField(),
                queueTableSchema.getAttemptField(),
                queueTableSchema.getReenqueueAttemptField(),
                queueTableSchema.getTotalAttemptField(),
                queueTableSchema.getCreatedAtField(),
                queueTableSchema.getNextProcessAtField(),
                queueTableSchema.getExtFields().isEmpty()
                        ? "" :
                        queueTableSchema
                                .getExtFields()
                                .stream()
                                .collect(Collectors.joining(", ", ", ", "")),
                location.getTableName(),
                queueTableSchema.getIdField()
        );
    }

    private static class RowIdLocker {
        private final Map<String, Set<Long>> lockedRowIds = new ConcurrentHashMap<>();

        public Long lock(final String queueName,
                         final Function<Set<Long>, Long> taskIdExtractor) {

            final AtomicReference<Long> atomicReference = new AtomicReference<>();
            lockedRowIds
                    .compute(queueName, (key, rowIds) -> {
                        Set<Long> idSet = rowIds == null ? new HashSet<>() : rowIds;

                        Long taskId = taskIdExtractor.apply(idSet);
                        if (taskId == null) {
                            return idSet;
                        } else {
                            atomicReference.set(taskId);
                        }

                        idSet.add(taskId);
                        return idSet;
                    });

            return atomicReference.get();
        }

        public void unlock(final String queueName, final Long taskId) {
            lockedRowIds
                    .computeIfPresent(queueName, (key, rowIds) -> {
                        rowIds.remove(taskId);
                        return rowIds;
                    });
        }
    }

    private static ZonedDateTime getZonedDateTime(final ResultSet rs, final String time) throws SQLException {
        return ZonedDateTime.ofInstant(rs.getTimestamp(time).toInstant(), ZoneId.systemDefault());
    }

    private static String getNextProcessTimeSql(final @Nonnull TaskRetryType taskRetryType,
                                                final QueueTableSchema queueTableSchema) {
        Objects.requireNonNull(taskRetryType, "retry type must be not null");
        switch (taskRetryType) {
            case GEOMETRIC_BACKOFF:
                return String.format("TIMESTAMPADD(SECOND, POWER(2, %s) * :retryInterval , NOW())", queueTableSchema.getAttemptField());
            case ARITHMETIC_BACKOFF:
                return String.format("TIMESTAMPADD(SECOND, (1 + %s * 2) * :retryInterval, NOW())", queueTableSchema.getAttemptField());
            case LINEAR_BACKOFF:
                return "TIMESTAMPADD(SECOND, :retryInterval, NOW())";
            default:
                throw new IllegalStateException("unknown retry type: " + taskRetryType);
        }
    }
}
