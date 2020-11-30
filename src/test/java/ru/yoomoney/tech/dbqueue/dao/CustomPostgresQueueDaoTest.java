package ru.yoomoney.tech.dbqueue.dao;

import org.junit.BeforeClass;
import ru.yoomoney.tech.dbqueue.utils.PostgresDatabaseInitializer;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class CustomPostgresQueueDaoTest extends QueueDaoTest {

    @BeforeClass
    public static void beforeClass() {
        PostgresDatabaseInitializer.initialize();
    }

    public CustomPostgresQueueDaoTest() {
        super(new PostgresQueueDao(PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.CUSTOM_SCHEMA),
                PostgresDatabaseInitializer.CUSTOM_TABLE_NAME, PostgresDatabaseInitializer.CUSTOM_SCHEMA,
                PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.getTransactionTemplate());
    }
}
