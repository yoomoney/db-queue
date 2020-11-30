package ru.yoomoney.tech.dbqueue.dao;

import org.junit.BeforeClass;
import ru.yoomoney.tech.dbqueue.utils.PostgresDatabaseInitializer;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class DefaultPostgresQueueDaoTest extends QueueDaoTest {

    @BeforeClass
    public static void beforeClass() {
        PostgresDatabaseInitializer.initialize();
    }

    public DefaultPostgresQueueDaoTest() {
        super(new PostgresQueueDao(PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.DEFAULT_SCHEMA),
                PostgresDatabaseInitializer.DEFAULT_TABLE_NAME, PostgresDatabaseInitializer.DEFAULT_SCHEMA,
                PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.getTransactionTemplate());
    }
}
