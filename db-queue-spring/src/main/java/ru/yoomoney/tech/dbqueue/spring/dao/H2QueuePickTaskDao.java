package ru.yoomoney.tech.dbqueue.spring.dao;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.dao.PickTaskSettings;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.TaskRetryType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class H2QueuePickTaskDao implements QueuePickTaskDao {
    private final JdbcOperations jdbcTemplate;
    private final QueueTableSchema queueTableSchema;
    private final PickTaskSettings pickTaskSettings;

    public H2QueuePickTaskDao(@Nonnull final JdbcOperations jdbcOperations,
                              @Nonnull final QueueTableSchema queueTableSchema,
                              @Nonnull final PickTaskSettings pickTaskSettings,
                              @Nonnull final TransactionOperations transactionTemplate) {

        this.jdbcTemplate = Objects.requireNonNull(jdbcOperations, "jdbc template can't be null");
        this.queueTableSchema = Objects.requireNonNull(queueTableSchema, "table schema can't be null");
        this.pickTaskSettings = Objects.requireNonNull(pickTaskSettings, "settings can't be null");
        Objects.requireNonNull(transactionTemplate, "transaction template must be not null");

        transactionTemplate.executeWithoutResult(transactionStatus -> {
            jdbcTemplate.update(
                    String.format(
                            "CREATE ALIAS IF NOT EXISTS PICK_TASK FOR \"%s.pickTask\"",
                            PickerProcedure.class.getName()));
        });
    }

    @Nullable
    @Override
    public TaskRecord pickTask(@Nonnull final QueueLocation location) {
        Objects.requireNonNull(location, "location can't be null");

        return jdbcTemplate.query(
                PickerProcedure.createPickQuery(location, pickTaskSettings, queueTableSchema),
                (ResultSet rs) -> {
                    if (!(rs.next() && rs.getMetaData().getColumnCount() > 1))
                        return null;

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

                }, location.getQueueId().asString(), pickTaskSettings.getRetryInterval().getSeconds());
    }

    public static class PickerProcedure {
        private static final Map<String, PickQuery> pickTaskSqlCache = new ConcurrentHashMap<>();
        private static final RowIdLocker rowIdLocker = new RowIdLocker();

        public static ResultSet pickTask(final Connection connection,
                                         final String queueName,
                                         final long retryInterval) throws SQLException {
            final PickQuery pickQuery = pickTaskSqlCache.get(queueName);
            Objects.requireNonNull(pickQuery, "A pick query can't be null");

            final Savepoint savepoint = connection.setSavepoint("start-picking-task-point");

            final Long taskId = rowIdLocker.lock(queueName, rowIds -> {
                try {

                    final String sql;
                    if (rowIds.isEmpty())
                        sql = String.format(pickQuery.selectSql, " (1 = 1) ");
                    else
                        sql = String.format(
                                pickQuery.selectSql,
                                " _ROWID_ IN " + rowIds
                                        .stream()
                                        .map(Object::toString)
                                        .collect(Collectors.joining(",", "(", ")")));

                    PreparedStatement chooseStatement = connection.prepareStatement(sql);
                    chooseStatement.setString(1, queueName);
                    ResultSet chooseResultSet = chooseStatement.executeQuery();
                    if (!chooseResultSet.next())
                        return null;

                    return chooseResultSet.getLong(1);
                } catch (Exception e) {
                    throw new IllegalStateException("can't select pick task", e);
                }
            });

            if (taskId == null)
                return null;

            try {
                PreparedStatement updateStatement = connection.prepareStatement(pickQuery.updateSql);
                updateStatement.setLong(1, retryInterval);
                updateStatement.setLong(2, taskId);
                int updatedRowCount = updateStatement.executeUpdate();
                if (updatedRowCount != 1)
                    throw new IllegalStateException("Something wrong went here. Only row must be updated, not more!");

                final PreparedStatement selectStatement = connection.prepareStatement(pickQuery.returnSql);
                selectStatement.setLong(1, taskId);
                final ResultSet resultSet = selectStatement.executeQuery();

                connection.commit();

                return resultSet;
            } catch (SQLException e) {
                connection.rollback(savepoint);
                throw new RuntimeException("can't update task", e);
            } finally {
                rowIdLocker.unlock(queueName, taskId);
            }
        }

        private static String createPickQuery(final QueueLocation location,
                                              final PickTaskSettings pickTaskSettings,
                                              final QueueTableSchema queueTableSchema) {

            pickTaskSqlCache.computeIfAbsent(location.getQueueId().asString(), ignoredKey -> {

                final String selectSql = String.format("" +
                                "SELECT %s " +
                                "FROM %s " +
                                "WHERE %s = ? " +
                                "  AND %s <= now() " +
                                "  AND %%s " +
                                "ORDER BY %s ASC " +
                                "LIMIT 1 " +
                                "FOR UPDATE ",
                        queueTableSchema.getIdField(),
                        location.getTableName(),
                        queueTableSchema.getQueueNameField(),
                        queueTableSchema.getNextProcessAtField(),
                        queueTableSchema.getNextProcessAtField()
                );

                final String updateSql = String.format("" +
                                "UPDATE %s " +
                                "SET " +
                                "  %s = %s, " +
                                "  %s = %s + 1, " +
                                "  %s = %s + 1 " +
                                "WHERE %s = ? ",
                        location.getTableName(),
                        queueTableSchema.getNextProcessAtField(),
                        getNextProcessTimeSql(pickTaskSettings.getRetryType(), queueTableSchema),
                        queueTableSchema.getAttemptField(),
                        queueTableSchema.getAttemptField(),
                        queueTableSchema.getTotalAttemptField(),
                        queueTableSchema.getTotalAttemptField(),
                        queueTableSchema.getIdField()
                );

                final String returnSql = String.format("" +
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
                                "WHERE %s = ? ",
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

                return new PickQuery(selectSql, updateSql, returnSql);
            });

            return "call PICK_TASK(?, ?)";
        }

        private static class PickQuery {
            private final String selectSql;
            private final String updateSql;
            private final String returnSql;

            private PickQuery(final String selectSql,
                              final String updateSql,
                              final String returnSql) {
                this.selectSql = selectSql;
                this.updateSql = updateSql;
                this.returnSql = returnSql;
            }
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
                            if (taskId == null)
                                return idSet;
                            else
                                atomicReference.set(taskId);

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
    }


    private static ZonedDateTime getZonedDateTime(final ResultSet rs, final String time) throws SQLException {
        return ZonedDateTime.ofInstant(rs.getTimestamp(time).toInstant(), ZoneId.systemDefault());
    }

    private static String getNextProcessTimeSql(final @Nonnull TaskRetryType taskRetryType,
                                                final QueueTableSchema queueTableSchema) {
        Objects.requireNonNull(taskRetryType, "retry type can't be null");
        switch (taskRetryType) {
            case GEOMETRIC_BACKOFF:
                return String.format("TIMESTAMPADD(SECOND, POWER(2, %s) * ? , NOW())", queueTableSchema.getAttemptField());
            case ARITHMETIC_BACKOFF:
                return String.format("TIMESTAMPADD(SECOND, (1 + %s * 2) * ?, NOW())", queueTableSchema.getAttemptField());
            case LINEAR_BACKOFF:
                return "TIMESTAMPADD(SECOND, ?, NOW())";
            default:
                throw new IllegalStateException("unknown retry type: " + taskRetryType);
        }
    }
}
