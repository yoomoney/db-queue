package ru.yoomoney.tech.dbqueue.dao;

import org.junit.BeforeClass;
import ru.yoomoney.tech.dbqueue.utils.MssqlDatabaseInitializer;

/**
 * @author Oleg Kandaurov
 * @author Behrooz Shabani
 * @since 25.01.2020
 */
public class DefaultMssqlQueueDaoTest extends QueueDaoTest {

    @BeforeClass
    public static void beforeClass() {
        MssqlDatabaseInitializer.initialize();
    }

    public DefaultMssqlQueueDaoTest() {
        super(new MssqlQueueDao(MssqlDatabaseInitializer.getJdbcTemplate(), MssqlDatabaseInitializer.DEFAULT_SCHEMA),
                MssqlDatabaseInitializer.DEFAULT_TABLE_NAME, MssqlDatabaseInitializer.DEFAULT_SCHEMA,
                MssqlDatabaseInitializer.getJdbcTemplate(), MssqlDatabaseInitializer.getTransactionTemplate());
    }
}
