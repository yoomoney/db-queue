package ru.yoomoney.tech.dbqueue.utils;

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.TestcontainersConfiguration;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;

import java.util.Collections;
import java.util.Optional;

/**
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public class PostgresDatabaseInitializer {

    public static final String DEFAULT_TABLE_NAME = "queue_default";
    public static final String DEFAULT_TABLE_NAME_WO_INC = "queue_default_wo_inc";
    public static final String CUSTOM_TABLE_NAME = "queue_custom";
    public static final QueueTableSchema DEFAULT_SCHEMA = QueueTableSchema.builder().build();
    public static final QueueTableSchema CUSTOM_SCHEMA = QueueTableSchema.builder()
            .withIdField("qid")
            .withQueueNameField("qn")
            .withPayloadField("pl")
            .withCreatedAtField("ct")
            .withNextProcessAtField("pt")
            .withAttemptField("at")
            .withReenqueueAttemptField("rat")
            .withTotalAttemptField("tat")
            .withExtFields(Collections.singletonList("trace"))
            .build();

    private static final String PG_CUSTOM_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  qid      BIGSERIAL PRIMARY KEY,\n" +
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
            "  ON %s (qn, pt, qid DESC);\n" +
            "\n";

    private static final String PG_DEFAULT_TABLE_DDL = "CREATE TABLE %s (\n" +
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

    private static final String PG_DEFAULT_WO_INC_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id                BIGINT PRIMARY KEY,\n" +
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

    private static JdbcTemplate pgJdbcTemplate;
    private static TransactionTemplate pgTransactionTemplate;

    public static synchronized void initialize() {
        if (pgJdbcTemplate != null) {
            return;
        }

        String ryukImage = Optional.ofNullable(System.getProperty("testcontainers.ryuk.container.image"))
                                .orElse("quay.io/testcontainers/ryuk:0.2.3");
        TestcontainersConfiguration.getInstance()
                .updateGlobalConfig("ryuk.container.image", ryukImage);

        String postgresImage = Optional.ofNullable(System.getProperty("testcontainers.postgresql.container.image"))
                .orElse("postgres:9.5");
        PostgreSQLContainer<?> dbContainer = new PostgreSQLContainer<>(postgresImage);
        dbContainer.withEnv("POSTGRES_INITDB_ARGS", "--nosync");
        dbContainer.withCommand("postgres -c fsync=off -c full_page_writes=off -c synchronous_commit=off");
        dbContainer.start();
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(dbContainer.getJdbcUrl());
        dataSource.setPassword(dbContainer.getPassword());
        dataSource.setUser(dbContainer.getUsername());
        pgJdbcTemplate = new JdbcTemplate(dataSource);
        pgTransactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        pgTransactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        pgTransactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);

        executeDdl("CREATE SEQUENCE tasks_seq START 1");
        createTable(PG_DEFAULT_WO_INC_TABLE_DDL, DEFAULT_TABLE_NAME_WO_INC);
        createTable(PG_DEFAULT_TABLE_DDL, DEFAULT_TABLE_NAME);
        createTable(PG_CUSTOM_TABLE_DDL, CUSTOM_TABLE_NAME);
    }

    public static void createDefaultTable(String tableName) {
        createTable(PG_DEFAULT_TABLE_DDL, tableName);
    }

    private static void createTable(String ddlTemplate, String tableName) {
        initialize();
        executeDdl(String.format(ddlTemplate, tableName, tableName, tableName));
    }

    private static void executeDdl(String ddl) {
        initialize();
        getTransactionTemplate().execute(status -> {
            getJdbcTemplate().execute(ddl);
            return new Object();
        });
    }

    public static JdbcTemplate getJdbcTemplate() {
        initialize();
        return pgJdbcTemplate;
    }

    public static TransactionTemplate getTransactionTemplate() {
        initialize();
        return pgTransactionTemplate;
    }
}
