package ru.yandex.money.common.dbqueue.utils;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.TestcontainersConfiguration;
import ru.yandex.money.common.dbqueue.config.QueueTableSchema;

import java.net.URI;
import java.util.Collections;
import java.util.Optional;

/**
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public class MssqlDatabaseInitializer {

    public static final String DEFAULT_TABLE_NAME = "queue_default";
    public static final String DEFAULT_TABLE_NAME_WO_IDENT = "queue_default_wo_ident";
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

    private static final String MS_CUSTOM_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  qid    int identity(1,1) not null,\n" +
            "  qn    varchar(127) not null,\n" +
            "  pl    text,\n" +
            "  ct    datetimeoffset not null default SYSDATETIMEOFFSET(),\n" +
            "  pt    datetimeoffset not null default SYSDATETIMEOFFSET(),\n" +
            "  at    integer not null         default 0,\n" +
            "  rat   integer not null         default 0,\n" +
            "  tat   integer not null         default 0,\n" +
            "  trace text \n" +
            "  primary key (qid)\n" +
            ");" +
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (qn, pt, qid DESC);\n" +
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

    private static final String MS_DEFAULT_WO_IDENT_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id                int not null,\n" +
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

    private static JdbcTemplate msJdbcTemplate;
    private static TransactionTemplate msTransactionTemplate;


    public static synchronized void initialize() {
        if (msJdbcTemplate != null) {
            return;
        }
        SQLServerDataSource dataSource;

        String ryukImage = Optional.ofNullable(System.getProperty("testcontainers.ryuk.container.image"))
                                .orElse("quay.io/testcontainers/ryuk:0.2.3");
        TestcontainersConfiguration.getInstance()
                .updateGlobalConfig("ryuk.container.image", ryukImage);

        String mssqlImage = Optional.ofNullable(System.getProperty("testcontainers.mssql.container.image"))
                .orElse("mcr.microsoft.com/mssql/server:2019-CU1-ubuntu-16.04");

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

        executeDdl("CREATE SEQUENCE tasks_seq START WITH 1");
        createTable(MS_DEFAULT_TABLE_DDL, DEFAULT_TABLE_NAME);
        createTable(MS_DEFAULT_WO_IDENT_TABLE_DDL, DEFAULT_TABLE_NAME_WO_IDENT);
        createTable(MS_CUSTOM_TABLE_DDL, CUSTOM_TABLE_NAME);
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
        return msJdbcTemplate;
    }

    public static TransactionTemplate getTransactionTemplate() {
        initialize();
        return msTransactionTemplate;
    }
}
