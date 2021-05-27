package ru.yoomoney.tech.dbqueue.spring.dao.utils;

import org.h2.jdbcx.JdbcDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;

import java.util.Collections;


public class H2DatabaseInitializer {

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

    private static final String H2_CUSTOM_TABLE_DDL = "" +
            "CREATE TABLE %s (\n" +
            "  qid      BIGSERIAL PRIMARY KEY,\n" +
            "  qn    TEXT NOT NULL,\n" +
            "  pl    TEXT,\n" +
            "  ct    TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  pt    TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  at    INTEGER                  DEFAULT 0,\n" +
            "  rat   INTEGER                  DEFAULT 0,\n" +
            "  tat   INTEGER                  DEFAULT 0,\n" +
            "  trace TEXT \n" +
            ");";

    private static final String H2_DEFAULT_TABLE_DDL = "" +
            "CREATE TABLE %s (\n" +
            "  id                BIGSERIAL PRIMARY KEY,\n" +
            "  queue_name        TEXT NOT NULL,\n" +
            "  payload           TEXT,\n" +
            "  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  attempt           INTEGER                  DEFAULT 0,\n" +
            "  reenqueue_attempt INTEGER                  DEFAULT 0,\n" +
            "  total_attempt     INTEGER                  DEFAULT 0\n" +
            ");";

    private static final String H2_DEFAULT_WO_INC_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id                BIGINT PRIMARY KEY,\n" +
            "  queue_name        TEXT NOT NULL,\n" +
            "  payload           TEXT,\n" +
            "  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  attempt           INTEGER                  DEFAULT 0,\n" +
            "  reenqueue_attempt INTEGER                  DEFAULT 0,\n" +
            "  total_attempt     INTEGER                  DEFAULT 0\n" +
            ");";

    private static JdbcTemplate h2JdbcTemplate;
    private static TransactionTemplate h2TransactionTemplate;

    public static synchronized void initialize() {
        if (h2JdbcTemplate != null) {
            return;
        }

        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setUrl("jdbc:h2:~/test");
        h2JdbcTemplate = new JdbcTemplate(dataSource);
        h2TransactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        h2TransactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        h2TransactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);

        h2JdbcTemplate.execute("DROP ALL OBJECTS DELETE FILES");
        executeDdl("CREATE SEQUENCE tasks_seq START 1");
        createTable(H2_DEFAULT_WO_INC_TABLE_DDL, DEFAULT_TABLE_NAME_WO_INC);
        createTable(H2_DEFAULT_TABLE_DDL, DEFAULT_TABLE_NAME);
        createTable(H2_CUSTOM_TABLE_DDL, CUSTOM_TABLE_NAME);
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
        return h2JdbcTemplate;
    }

    public static TransactionTemplate getTransactionTemplate() {
        initialize();
        return h2TransactionTemplate;
    }
}
