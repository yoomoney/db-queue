package ru.yoomoney.tech.dbqueue.internal.dao;

import org.junit.BeforeClass;
import ru.yoomoney.tech.dbqueue.dao.PostgresQueueDao;
import ru.yoomoney.tech.dbqueue.internal.pick.PostgresQueuePickTaskDao;
import ru.yoomoney.tech.dbqueue.utils.PostgresDatabaseInitializer;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class CustomPostgresQueuePickTaskDaoTest extends QueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        PostgresDatabaseInitializer.initialize();
    }

    public CustomPostgresQueuePickTaskDaoTest() {
        super(new PostgresQueueDao(PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.CUSTOM_SCHEMA),
                pickTaskSettings -> new PostgresQueuePickTaskDao(PostgresDatabaseInitializer.getJdbcTemplate(),
                        PostgresDatabaseInitializer.CUSTOM_SCHEMA, pickTaskSettings),
                PostgresDatabaseInitializer.CUSTOM_TABLE_NAME, PostgresDatabaseInitializer.CUSTOM_SCHEMA,
                PostgresDatabaseInitializer.getJdbcTemplate(), PostgresDatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "now()";
    }
}
