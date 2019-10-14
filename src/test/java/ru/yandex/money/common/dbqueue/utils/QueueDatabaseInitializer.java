package ru.yandex.money.common.dbqueue.utils;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yandex.money.common.dbqueue.config.QueueTableSchema;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Collections;

/**
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public class QueueDatabaseInitializer {

    public static final String DEFAULT_TABLE_NAME = "queue_default";
    public static final String CUSTOM_TABLE_NAME = "queue_custom";
    public static final QueueTableSchema DEFAULT_SCHEMA = QueueTableSchema.builder().build();
    public static final QueueTableSchema CUSTOM_SCHEMA = QueueTableSchema.builder()
            .withQueueNameField("qn")
            .withPayloadField("pl")
            .withCreatedAtField("ct")
            .withNextProcessAtField("pt")
            .withAttemptField("at")
            .withReenqueueAttemptField("rat")
            .withTotalAttemptField("tat")
            .withExtFields(Collections.singletonList("trace"))
            .build();

    private static final String CUSTOM_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id      BIGSERIAL PRIMARY KEY,\n" +
            "  qn    TEXT NOT NULL,\n" +
            "  pl    TEXT,\n" +
            "  ct    TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  pt    TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  at    INTEGER                  DEFAULT 0,\n" +
            "  rat   INTEGER                  DEFAULT 0,\n" +
            "  tat   INTEGER                  DEFAULT 0,\n" +
            "  trace TEXT \n" +
            ");" +
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (qn, pt, id DESC);\n" +
            "\n";

    private static final String DEFAULT_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id                BIGSERIAL PRIMARY KEY,\n" +
            "  queue_name        TEXT NOT NULL,\n" +
            "  payload           TEXT,\n" +
            "  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  attempt           INTEGER                  DEFAULT 0,\n" +
            "  reenqueue_attempt INTEGER                  DEFAULT 0,\n" +
            "  total_attempt     INTEGER                  DEFAULT 0\n" +
            ");" +
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (queue_name, next_process_at, id DESC);\n" +
            "\n";

    private static JdbcTemplate jdbcTemplate;
    private static TransactionTemplate transactionTemplate;

    public static synchronized void initialize() {
        if (jdbcTemplate != null) {
            return;
        }
        EmbeddedPostgres postgres;
        try {
            // https://stackoverflow.com/questions/9407442/optimise-postgresql-for-fast-testing
            postgres = EmbeddedPostgres.builder()
                    .setServerConfig("fsync", "off")
                    .setServerConfig("full_page_writes", "off")
                    .setServerConfig("synchronous_commit", "off")
                    .start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DataSource dataSource = postgres.getPostgresDatabase();
        jdbcTemplate = new JdbcTemplate(dataSource);
        transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);

        createTable(DEFAULT_TABLE_DDL, DEFAULT_TABLE_NAME);
        createTable(CUSTOM_TABLE_DDL, CUSTOM_TABLE_NAME);
    }

    private static void createTable(String ddlTemplate, String tableName) {
        initialize();
        String tableDDl = String.format(ddlTemplate, tableName, tableName, tableName);
        transactionTemplate.execute(status -> {
            jdbcTemplate.execute(tableDDl);
            return new Object();
        });
    }

    public static void createDefaultTable(String tableName) {
        createTable(DEFAULT_TABLE_DDL, tableName);
    }

    public static JdbcTemplate getJdbcTemplate() {
        initialize();
        return jdbcTemplate;
    }

    public static TransactionTemplate getTransactionTemplate() {
        initialize();
        return transactionTemplate;
    }
}
