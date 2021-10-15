package ru.yoomoney.tech.dbqueue.spring.dao;

import org.junit.BeforeClass;
import ru.yoomoney.tech.dbqueue.spring.dao.utils.MssqlDatabaseInitializer;

/**
 * @author Oleg Kandaurov
 * @author Behrooz Shabani
 * @since 25.01.2020
 */
public class CustomMssqlQueuePickTaskDaoTest extends QueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        MssqlDatabaseInitializer.initialize();
    }

    public CustomMssqlQueuePickTaskDaoTest() {
        super(new MssqlQueueDao(MssqlDatabaseInitializer.getJdbcTemplate(), MssqlDatabaseInitializer.CUSTOM_SCHEMA),
                (queueLocation, failureSettings) -> new MssqlQueuePickTaskDao(MssqlDatabaseInitializer.getJdbcTemplate(),
                        MssqlDatabaseInitializer.CUSTOM_SCHEMA, queueLocation, failureSettings),
                MssqlDatabaseInitializer.CUSTOM_TABLE_NAME, MssqlDatabaseInitializer.CUSTOM_SCHEMA,
                MssqlDatabaseInitializer.getJdbcTemplate(), MssqlDatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "SYSDATETIMEOFFSET()";
    }
}
