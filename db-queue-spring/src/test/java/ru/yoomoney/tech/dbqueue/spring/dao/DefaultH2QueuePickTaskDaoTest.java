package ru.yoomoney.tech.dbqueue.spring.dao;

import org.junit.BeforeClass;
import ru.yoomoney.tech.dbqueue.spring.dao.utils.H2DatabaseInitializer;

public class DefaultH2QueuePickTaskDaoTest extends QueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        H2DatabaseInitializer.initialize();
    }

    public DefaultH2QueuePickTaskDaoTest() {
        super(
                new H2QueueDao(H2DatabaseInitializer.getJdbcTemplate(), H2DatabaseInitializer.DEFAULT_SCHEMA),
                pickTaskSettings ->
                        new H2QueuePickTaskDao(
                                H2DatabaseInitializer.getJdbcTemplate(),
                                H2DatabaseInitializer.DEFAULT_SCHEMA,
                                pickTaskSettings),
                H2DatabaseInitializer.DEFAULT_TABLE_NAME, H2DatabaseInitializer.DEFAULT_SCHEMA,
                H2DatabaseInitializer.getJdbcTemplate(), H2DatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "now()";
    }
}
