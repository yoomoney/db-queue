package ru.yandex.money.common.dbqueue.utils;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.io.IOException;

/**
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public class QueueDatabaseInitializer {

    public static final String DEFAULT_TABLE_NAME = "queue_test";

    private static final String QUEUE_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id                BIGSERIAL PRIMARY KEY,\n" +
            "  queue_name        VARCHAR(128) NOT NULL,\n" +
            "  task              TEXT,\n" +
            "  create_time       TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  process_time      TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  attempt           INTEGER                  DEFAULT 0,\n" +
            "  reenqueue_attempt INTEGER                  DEFAULT 0,\n" +
            "  total_attempt     INTEGER                  DEFAULT 0,\n" +
            "  actor             VARCHAR(128),\n" +
            "  log_timestamp     TEXT\n" +
            ");" +
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON queue_test (queue_name, process_time, id DESC);\n" +
            "\n" +
            "CREATE INDEX %s_name_actor_desc_idx\n" +
            "  ON queue_test (actor, queue_name);";

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
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        createTable(DEFAULT_TABLE_NAME);
    }

    public static void createTable(String tableName) {
        initialize();
        String tableDDl = String.format(QUEUE_TABLE_DDL, tableName, tableName, tableName);
        transactionTemplate.execute(status -> {
            jdbcTemplate.execute(tableDDl);
            return new Object();
        });
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
