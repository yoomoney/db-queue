package ru.yoomoney.tech.dbqueue.spring.dao;

import org.junit.BeforeClass;
import ru.yoomoney.tech.dbqueue.spring.dao.utils.MssqlDatabaseInitializer;

/**
 * @author Oleg Kandaurov
 * @author Behrooz Shabani
 * @since 25.01.2020
 */
public class DefaultMssqlQueuePickTaskDaoTest extends QueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        MssqlDatabaseInitializer.initialize();
    }

    public DefaultMssqlQueuePickTaskDaoTest() {
        super(new MssqlQueueDao(MssqlDatabaseInitializer.getJdbcTemplate(), MssqlDatabaseInitializer.DEFAULT_SCHEMA),
                (queueLocation, failureSettings) -> new MssqlQueuePickTaskDao(MssqlDatabaseInitializer.getJdbcTemplate(),
                        MssqlDatabaseInitializer.DEFAULT_SCHEMA, queueLocation, failureSettings),
                MssqlDatabaseInitializer.DEFAULT_TABLE_NAME, MssqlDatabaseInitializer.DEFAULT_SCHEMA,
                MssqlDatabaseInitializer.getJdbcTemplate(), MssqlDatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "SYSDATETIMEOFFSET()";
    }
}
