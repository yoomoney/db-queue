package ru.yandex.money.common.dbqueue.internal.pick;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.JdbcOperations;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.config.QueueTableSchema;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.TaskRetryType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Database access object to pick tasks in the queue for Oracle database type.
 *
 * @author Oleg Kandaurov
 * @since 15.05.2020
 */
@SuppressFBWarnings({"UCPM_USE_CHARACTER_PARAMETERIZED_METHOD",
        "ISB_INEFFICIENT_STRING_BUFFERING"})
public class Oracle11QueuePickTaskDao implements QueuePickTaskDao {

    private final Map<QueueLocation, String> pickTaskSqlCache = new ConcurrentHashMap<>();

    @Nonnull
    private final JdbcOperations jdbcTemplate;
    @Nonnull
    private final QueueTableSchema queueTableSchema;
    @Nonnull
    private final PickTaskSettings pickTaskSettings;

    public Oracle11QueuePickTaskDao(@Nonnull JdbcOperations jdbcTemplate,
                                    @Nonnull QueueTableSchema queueTableSchema,
                                    @Nonnull PickTaskSettings pickTaskSettings) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate);
        this.queueTableSchema = Objects.requireNonNull(queueTableSchema);
        this.pickTaskSettings = Objects.requireNonNull(pickTaskSettings);

    }

    @Nullable
    @Override
    @SuppressFBWarnings("SQL_INJECTION_SPRING_JDBC")
    public TaskRecord pickTask(@Nonnull QueueLocation location) {
        PickTaskCallableStatement pickTaskStatement = new PickTaskCallableStatement(queueTableSchema, location, pickTaskSettings);

        return jdbcTemplate.execute(pickTaskSqlCache.computeIfAbsent(location, this::createPickTaskSql), pickTaskStatement);
    }


    private static class PickTaskCallableStatement implements CallableStatementCallback<TaskRecord> {

        private final QueueLocation queueLocation;
        private final PickTaskSettings pickTaskSettings;
        private final QueueTableSchema queueTableSchema;

        public PickTaskCallableStatement(QueueTableSchema queueTableSchema,
                                         QueueLocation queueLocation,
                                         PickTaskSettings pickTaskSettings) {
            this.queueLocation = queueLocation;
            this.pickTaskSettings = pickTaskSettings;
            this.queueTableSchema = queueTableSchema;
        }

        @Override
        public TaskRecord doInCallableStatement(CallableStatement cs) throws SQLException, DataAccessException {
            int inputIndex = 1;
            cs.setString(inputIndex++, queueLocation.getQueueId().asString());
            cs.setLong(inputIndex++, pickTaskSettings.getRetryInterval().getSeconds());
            cs.registerOutParameter(inputIndex++, java.sql.Types.BIGINT);
            cs.registerOutParameter(inputIndex++, java.sql.Types.CLOB);
            cs.registerOutParameter(inputIndex++, java.sql.Types.BIGINT);
            cs.registerOutParameter(inputIndex++, java.sql.Types.BIGINT);
            cs.registerOutParameter(inputIndex++, java.sql.Types.BIGINT);
            cs.registerOutParameter(inputIndex++, java.sql.Types.TIMESTAMP);
            cs.registerOutParameter(inputIndex++, java.sql.Types.TIMESTAMP);

            for (String ignored : queueTableSchema.getExtFields()) {
                cs.registerOutParameter(inputIndex++, Types.VARCHAR);
            }

            cs.execute();

            int resultIndex = 3;
            long id = cs.getLong(resultIndex++);
            if (id == 0L) {
                return null;
            }
            TaskRecord.Builder builder = TaskRecord.builder()
                    .withId(id)
                    .withPayload(cs.getString(resultIndex++))
                    .withAttemptsCount(cs.getLong(resultIndex++))
                    .withReenqueueAttemptsCount(cs.getLong(resultIndex++))
                    .withTotalAttemptsCount(cs.getLong(resultIndex++))
                    .withCreatedAt(getZonedDateTime(cs.getTimestamp(resultIndex++)))
                    .withNextProcessAt(getZonedDateTime(cs.getTimestamp(resultIndex++)));

            Map<String, String> extData = new HashMap<>(queueTableSchema.getExtFields().size());
            for (String field : queueTableSchema.getExtFields()) {
                extData.put(field, cs.getString(resultIndex++));
            }
            return builder.withExtData(extData).build();
        }

        private ZonedDateTime getZonedDateTime(Timestamp timestamp) {
            return ZonedDateTime.ofInstant(timestamp.toInstant(), ZoneId.systemDefault());
        }
    }


    @Nonnull
    private String getNextProcessTimeSql(@Nonnull TaskRetryType taskRetryType) {
        Objects.requireNonNull(taskRetryType);
        switch (taskRetryType) {
            case GEOMETRIC_BACKOFF:
                return "CURRENT_TIMESTAMP + power(2, " + "rattempt) * ? * (INTERVAL '1' SECOND)";
            case ARITHMETIC_BACKOFF:
                return "CURRENT_TIMESTAMP + (1 + (" + "rattempt * 2)) * ? * (INTERVAL '1' SECOND)";
            case LINEAR_BACKOFF:
                return "CURRENT_TIMESTAMP + ? * (INTERVAL '1' SECOND)";
            default:
                throw new IllegalStateException("unknown retry type: " + taskRetryType);
        }
    }

    private String createPickTaskSql(QueueLocation queueLocation) {
        StringBuilder declaration = new StringBuilder("DECLARE\n"
                + " rid " + queueLocation.getTableName() + "." + queueTableSchema.getIdField() + "%TYPE;\n"
                + " rpayload " + queueLocation.getTableName() + "." + queueTableSchema.getPayloadField() + "%TYPE;\n"
                + " rattempt " + queueLocation.getTableName() + "." + queueTableSchema.getAttemptField() + "%TYPE;\n"
                + " rreenqueue_attempt " + queueLocation.getTableName() + "." + queueTableSchema.getReenqueueAttemptField() + "%TYPE;\n"
                + " rtotal_attempt " + queueLocation.getTableName() + "." + queueTableSchema.getTotalAttemptField() + "%TYPE;\n"
                + " rcreated_at " + queueLocation.getTableName() + "." + queueTableSchema.getCreatedAtField() + "%TYPE;\n"
                + " rnext_process_at " + queueLocation.getTableName() + "." + queueTableSchema.getNextProcessAtField() + "%TYPE;\n");
        queueTableSchema.getExtFields().forEach(field ->
                declaration.append("r").append(field).append(" ")
                        .append(queueLocation.getTableName()).append(".").append(field).append("%TYPE;\n")
        );

        StringBuilder cursorSelect = new StringBuilder(" CURSOR c IS SELECT " +
                queueTableSchema.getIdField() + ", " +
                queueTableSchema.getPayloadField() + ", " +
                queueTableSchema.getAttemptField() + ", " +
                queueTableSchema.getReenqueueAttemptField() + ", " +
                queueTableSchema.getTotalAttemptField() + ", " +
                queueTableSchema.getCreatedAtField() + ", ");
        queueTableSchema.getExtFields().forEach(field ->
                cursorSelect.append(field).append(", ")
        );
        cursorSelect.append(queueTableSchema.getNextProcessAtField()).append(" ");

        final String fetchCursor = " FROM " + queueLocation.getTableName() + " "
                + " WHERE " + queueTableSchema.getQueueNameField() + " = ? AND "
                + queueTableSchema.getNextProcessAtField() + " <= CURRENT_TIMESTAMP"
                + " FOR UPDATE SKIP LOCKED;"
                + " BEGIN \n"
                + " OPEN c; \n"
                + " FETCH c INTO ";

        StringBuilder fetchParams = new StringBuilder("rid, " +
                "rpayload, " +
                "rattempt, " +
                "rreenqueue_attempt, " +
                "rtotal_attempt, " +
                "rcreated_at, ");
        queueTableSchema.getExtFields().forEach(field -> fetchParams.append("r").append(field).append(", "));
        fetchParams.append("rnext_process_at;\n");

        String updateSql = "IF (c%NOTFOUND) THEN \n"
                + " rid := 0;\n "
                + " END IF\n;"
                + " CLOSE c;\n"
                + " IF (rid > 0) THEN \n"
                + " rnext_process_at := " + getNextProcessTimeSql(pickTaskSettings.getRetryType()) + ";\n"
                + " rattempt := rattempt + 1;\n"
                + " rtotal_attempt := rtotal_attempt + 1;\n"
                + "   UPDATE " + queueLocation.getTableName() + " SET " +
                queueTableSchema.getNextProcessAtField() + " = rnext_process_at, " +
                queueTableSchema.getAttemptField() + " = rattempt,  " +
                queueTableSchema.getTotalAttemptField() + " = rtotal_attempt WHERE " + queueTableSchema.getIdField() + " = rid; \n"
                + " END IF;";

        StringBuilder returnParams = new StringBuilder("\n ? := rid; " +
                "\n ? := rpayload; " +
                "\n ? := rattempt; " +
                "\n ? := rreenqueue_attempt; " +
                "\n ? := rtotal_attempt; " +
                "\n ? := rcreated_at; " +
                "\n ? := rnext_process_at; ");
        queueTableSchema.getExtFields().forEach(field -> returnParams.append("\n ? := r").append(field).append("; "));
        returnParams.append("\n END; ");

        return declaration.toString() + cursorSelect + fetchCursor + fetchParams + updateSql + returnParams;
    }

}
