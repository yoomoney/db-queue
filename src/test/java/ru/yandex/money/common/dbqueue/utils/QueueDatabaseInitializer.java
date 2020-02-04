package ru.yandex.money.common.dbqueue.utils;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.MSSQLServerContainerProvider;
import org.testcontainers.utility.TestcontainersConfiguration;
import ru.yandex.money.common.dbqueue.config.QueueTableSchema;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.time.ZoneId;

/**
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public class QueueDatabaseInitializer {

    public enum DatabaseType {
        PG, MS
    }

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

    private static final String PG_CUSTOM_TABLE_DDL = "CREATE TABLE %s (\n" +
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

    private static final String MS_CUSTOM_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id    int identity(1,1) not null,\n" +
            "  qn    varchar(127) not null,\n" +
            "  pl    text,\n" +
            "  ct    datetimeoffset not null default SYSDATETIMEOFFSET(),\n" +
            "  pt    datetimeoffset not null default SYSDATETIMEOFFSET(),\n" +
            "  at    integer not null         default 0,\n" +
            "  rat   integer not null         default 0,\n" +
            "  tat   integer not null         default 0,\n" +
            "  trace text \n" +
            "  primary key (id)\n" +
            ");" +
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (qn, pt, id DESC);\n" +
            "\n";

    private static final String MS_DEFAULT_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id                int identity(1,1) not null,\n" +
            "  queue_name        varchar(127) not null,\n" +
            "  payload           text,\n" +
            "  created_at        datetimeoffset not null default SYSDATETIMEOFFSET(),\n" +
            "  next_process_at   datetimeoffset not null default SYSDATETIMEOFFSET(),\n" +
            "  attempt           integer not null         default 0,\n" +
            "  reenqueue_attempt integer not null         default 0,\n" +
            "  total_attempt     integer not null         default 0,\n" +
            "  primary key (id)\n" +
            ");" +
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (queue_name, next_process_at, id DESC);\n" +
            "\n";

    public static DatabaseType databaseType;

    private static JdbcTemplate pgJdbcTemplate;
    private static TransactionTemplate pgTransactionTemplate;

    private static JdbcTemplate msJdbcTemplate;
    private static TransactionTemplate msTransactionTemplate;

    public static synchronized void initialize() {
        switch (databaseType) {
            case PG:
                initializePG();
                break;
            case MS:
                initializeMS();
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    private static void initializePG() {
        if (pgJdbcTemplate != null) {
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
        pgJdbcTemplate = new JdbcTemplate(dataSource);
        pgTransactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        pgTransactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        pgTransactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);

        createTable(PG_DEFAULT_TABLE_DDL, DEFAULT_TABLE_NAME);
        createTable(PG_CUSTOM_TABLE_DDL, CUSTOM_TABLE_NAME);
    }

    private static void initializeMS() {
        if (msJdbcTemplate != null) {
            return;
        }
        SQLServerDataSource dataSource;

        String ryukImage = System.getProperty("testcontainers.ryuk.container.image");
        TestcontainersConfiguration.getInstance()
                .updateGlobalConfig("ryuk.container.image", ryukImage);

        String mssqlImage = System.getProperty("testcontainers.mssql.container.image");

        MSSQLServerContainer containerInstance = new MSSQLServerContainer<>(mssqlImage);
        containerInstance.start();
        URI uri = URI.create(containerInstance.getJdbcUrl().substring(5));
        dataSource = new SQLServerDataSource();
        dataSource.setServerName(uri.getHost());
        dataSource.setPortNumber(uri.getPort());
        dataSource.setUser(containerInstance.getUsername());
        dataSource.setPassword(containerInstance.getPassword());
        msJdbcTemplate = new JdbcTemplate(dataSource);
        msTransactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        msTransactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        msTransactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);

        createTable(MS_DEFAULT_TABLE_DDL, DEFAULT_TABLE_NAME);
        createTable(MS_CUSTOM_TABLE_DDL, CUSTOM_TABLE_NAME);
    }

    private static void createTable(String ddlTemplate, String tableName) {
        initialize();
        String tableDDl = String.format(ddlTemplate, tableName, tableName, tableName);
        getTransactionTemplate().execute(status -> {
            getJdbcTemplate().execute(tableDDl);
            return new Object();
        });
    }

    public static void createDefaultTable(String tableName) {
        createTable(PG_DEFAULT_TABLE_DDL, tableName);
    }

    public static JdbcTemplate getJdbcTemplate() {
        initialize();
        if (databaseType == DatabaseType.PG) {
            return pgJdbcTemplate;
        } else if (databaseType == DatabaseType.MS) {
            return msJdbcTemplate;
        }
        throw new RuntimeException("wrong initialization!");
    }

    public static TransactionTemplate getTransactionTemplate() {
        initialize();
        if (databaseType == DatabaseType.PG) {
            return pgTransactionTemplate;
        } else if (databaseType == DatabaseType.MS) {
            return msTransactionTemplate;
        }
        throw new RuntimeException("wrong initialization!");
    }
}
