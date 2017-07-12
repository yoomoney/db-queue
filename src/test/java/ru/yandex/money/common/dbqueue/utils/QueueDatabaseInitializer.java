package ru.yandex.money.common.dbqueue.utils;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import org.apache.commons.io.IOUtils;
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
                    .setServerConfig("fsync", "on")
                    .setServerConfig("synchronous_commit", "on")
                    .setServerConfig("full_page_writes", "off")
                    .start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DataSource dataSource = postgres.getPostgresDatabase();
        jdbcTemplate = new JdbcTemplate(dataSource);
        transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        String initSql;
        try {
            initSql = IOUtils.toString(QueueDatabaseInitializer.class.getResourceAsStream("/queue-ddl.sql"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        transactionTemplate.execute(status -> {
            jdbcTemplate.execute(initSql);
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
