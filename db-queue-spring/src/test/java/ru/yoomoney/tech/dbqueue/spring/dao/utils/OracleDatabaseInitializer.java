package ru.yoomoney.tech.dbqueue.spring.dao.utils;

import oracle.jdbc.pool.OracleConnectionPoolDataSource;
import oracle.jdbc.pool.OracleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.TestcontainersConfiguration;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.TimeZone;

/**
 * @author Oleg Kandaurov
 * @since 15.05.2020
 */
public class OracleDatabaseInitializer {


    static {
        // Oracle image has old timezone files so we make test independent of timezone
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    public static final String DEFAULT_TABLE_NAME = "queue_default";
    public static final String CUSTOM_TABLE_NAME = "queue_custom";
    public static final QueueTableSchema DEFAULT_SCHEMA = QueueTableSchema.builder().build();
    public static final QueueTableSchema CUSTOM_SCHEMA = QueueTableSchema.builder()
            .withIdField("qid")
            .withQueueNameField("qn")
            .withPayloadField("pl")
            .withCreatedAtField("ct")
            .withNextProcessAtField("pt")
            .withAttemptField("att")
            .withReenqueueAttemptField("rat")
            .withTotalAttemptField("tat")
            .withExtFields(Collections.singletonList("trace"))
            .build();

    private static final String ORA_CUSTOM_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  qid     NUMBER(38) NOT NULL PRIMARY KEY,\n" +
            "  qn     VARCHAR2(128) NOT NULL,\n" +
            "  pl     CLOB,\n" +
            "  ct     TIMESTAMP WITH LOCAL TIME ZONE DEFAULT CURRENT_TIMESTAMP,\n" +
            "  pt     TIMESTAMP WITH LOCAL TIME ZONE DEFAULT CURRENT_TIMESTAMP,\n" +
            "  att     NUMBER(38)                  DEFAULT 0,\n" +
            "  rat    NUMBER(38)                  DEFAULT 0,\n" +
            "  tat    NUMBER(38)                  DEFAULT 0,\n" +
            "  trace  VARCHAR2(512)                  DEFAULT 0\n" +
            ")";


    private static final String ORA_DEFAULT_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id                NUMBER(38) NOT NULL PRIMARY KEY,\n" +
            "  queue_name        VARCHAR2(128) NOT NULL,\n" +
            "  payload           CLOB,\n" +
            "  created_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,\n" +
            "  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,\n" +
            "  attempt           NUMBER(38)                  DEFAULT 0,\n" +
            "  reenqueue_attempt NUMBER(38)                  DEFAULT 0,\n" +
            "  total_attempt     NUMBER(38)                  DEFAULT 0\n" +
            ")";

    private static JdbcTemplate oraJdbcTemplate;
    private static TransactionTemplate oraTransactionTemplate;

    public static synchronized void initialize() {
        if (oraJdbcTemplate != null) {
            return;
        }

        String ryukImage = Optional.ofNullable(System.getProperty("testcontainers.ryuk.container.image"))
                .orElse("quay.io/testcontainers/ryuk:0.2.3");
        TestcontainersConfiguration.getInstance()
                .updateGlobalConfig("ryuk.container.image", ryukImage);

        String oracleImage = Optional.ofNullable(System.getProperty("testcontainers.oracle.container.image"))
                .orElse("wnameless/oracle-xe-11g-r2");

        OracleContainer dbContainer = new OracleContainer(oracleImage);
        dbContainer.start();
        addUserAndSchema(dbContainer, "oracle_test");
        OracleDataSource dataSource = getDataSource(dbContainer, "oracle_test");

        oraJdbcTemplate = new JdbcTemplate(dataSource);
        oraTransactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        oraTransactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        oraTransactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);

        executeDdl("CREATE SEQUENCE tasks_seq START WITH 1");
        createTable(ORA_DEFAULT_TABLE_DDL, DEFAULT_TABLE_NAME);
        createTable(ORA_CUSTOM_TABLE_DDL, CUSTOM_TABLE_NAME);
    }

    private static OracleDataSource getDataSource(OracleContainer dbContainer, String userName) {
        OracleConnectionPoolDataSource dataSource;
        try {
            dataSource = new OracleConnectionPoolDataSource();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }

        dataSource.setURL("jdbc:oracle:thin:" + userName + "/" + userName + "@localhost:" + dbContainer.getFirstMappedPort() + ":xe");
        return dataSource;
    }

    private static void addUserAndSchema(OracleContainer dbContainer, String userName) {
        Arrays.asList("CREATE USER " + userName + " IDENTIFIED BY " + userName + "",
                        "ALTER USER " + userName + " QUOTA unlimited ON SYSTEM",
                        "GRANT CREATE SESSION, CONNECT, RESOURCE, DBA TO " + userName,
                        "GRANT ALL PRIVILEGES TO " + userName)
                .forEach(s -> {
                    try (Connection connection = dbContainer.createConnection("")) {
                        try (PreparedStatement statement = connection.prepareStatement(s)) {
                            statement.executeUpdate();
                        }
                    } catch (Exception ex) {
                        throw new IllegalStateException(ex);
                    }
                });
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
        return oraJdbcTemplate;
    }

    public static TransactionTemplate getTransactionTemplate() {
        initialize();
        return oraTransactionTemplate;
    }
}
