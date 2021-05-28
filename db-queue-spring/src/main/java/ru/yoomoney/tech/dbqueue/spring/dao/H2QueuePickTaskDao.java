package ru.yoomoney.tech.dbqueue.spring.dao;

import org.springframework.jdbc.core.JdbcOperations;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.dao.PickTaskSettings;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.TaskRetryType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
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

public class H2QueuePickTaskDao implements QueuePickTaskDao {
    private final JdbcOperations jdbcTemplate;
    private final QueueTableSchema queueTableSchema;
    private final PickTaskSettings pickTaskSettings;

    public H2QueuePickTaskDao(@Nonnull final JdbcOperations jdbcOperations,
                              @Nonnull final QueueTableSchema queueTableSchema,
                              @Nonnull final PickTaskSettings pickTaskSettings) {

        this.jdbcTemplate = Objects.requireNonNull(jdbcOperations);
        this.queueTableSchema = Objects.requireNonNull(queueTableSchema);
        this.pickTaskSettings = Objects.requireNonNull(pickTaskSettings);

        jdbcTemplate.update(String.format("CREATE ALIAS IF NOT EXISTS PICK_TASK FOR \"%s.pickTask\"", PickerProcedure.class.getName()));
    }

    @Nullable
    @Override
    public TaskRecord pickTask(@Nonnull final QueueLocation location) {
        requireNonNull(location);

        return jdbcTemplate.query(
                PickerProcedure.createPickQuery(location, pickTaskSettings, queueTableSchema),
                (ResultSet rs) -> {
                    if (!rs.next() && rs.getRow() != 1) {
                        //noinspection ReturnOfNull
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

                }, location.getQueueId().asString(), pickTaskSettings.getRetryInterval().getSeconds());
    }

    public static class PickerProcedure {
        private static final Map<String, PickQuery> pickTaskSqlCache = new ConcurrentHashMap<>();

        public static ResultSet pickTask(final Connection connection,
                                         final String queueName,
                                         final long retryInterval) throws SQLException {

            PickQuery pickQuery = pickTaskSqlCache.get(queueName);
            Objects.requireNonNull(pickQuery, "pick query can't be null");

            PreparedStatement chooseStatement = connection.prepareStatement(pickQuery.selectSql);
            chooseStatement.setString(1, queueName);
            ResultSet chooseResultSet = chooseStatement.executeQuery();
            if (!chooseResultSet.next())
                return chooseResultSet;

            final long taskId = chooseResultSet.getLong(1);
            PreparedStatement updateStatement = connection.prepareStatement(pickQuery.updateSql);
            updateStatement.setLong(1, retryInterval);
            updateStatement.setLong(2, taskId);
            int updatedRowCount = updateStatement.executeUpdate();
            if (updatedRowCount != 1)
                throw new IllegalStateException("Something wrong went here! Only row must be updated, not more!");

            final PreparedStatement selectStatement = connection.prepareStatement(pickQuery.returnSql);
            selectStatement.setLong(1, taskId);
            return selectStatement.executeQuery();
        }

        private static String createPickQuery(QueueLocation location,
                                              PickTaskSettings pickTaskSettings,
                                              QueueTableSchema queueTableSchema) {

            pickTaskSqlCache.computeIfAbsent(location.getQueueId().asString(), ignoredKey -> {

                final String selectSql = String.format("" +
                                "SELECT %s " +
                                "FROM %s " +
                                "WHERE %s = ? " +
                                "  AND %s <= now() " +
                                "ORDER BY %s ASC " +
                                "LIMIT 1 " +
                                "FOR UPDATE",
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
                                        .map(field -> "q." + field)
                                        .collect(Collectors.joining(", ", ", ", "")),
                        location.getTableName(),
                        queueTableSchema.getIdField()
                );

                return new PickQuery(selectSql, updateSql, returnSql);
            });

            return "call PICK_TASK(?,?)";
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
    }


    private static ZonedDateTime getZonedDateTime(ResultSet rs, String time) throws SQLException {
        return ZonedDateTime.ofInstant(rs.getTimestamp(time).toInstant(), ZoneId.systemDefault());
    }


    @Nonnull
    private static String getNextProcessTimeSql(@Nonnull TaskRetryType taskRetryType, QueueTableSchema queueTableSchema) {
        Objects.requireNonNull(taskRetryType);
        switch (taskRetryType) {
            case GEOMETRIC_BACKOFF:
                return String.format("timestampadd(second, power(2, %s) * ? , now())", queueTableSchema.getAttemptField());
            case ARITHMETIC_BACKOFF:
                return String.format("timestampadd(second, (1 + %s * 2) * ?, now())", queueTableSchema.getAttemptField());
            case LINEAR_BACKOFF:
                return "timestampadd(second, ?, now())";
            default:
                throw new IllegalStateException("unknown retry type: " + taskRetryType);
        }
    }
}
