package ru.yoomoney.tech.dbqueue.spring.dao;

import org.junit.BeforeClass;
import ru.yoomoney.tech.dbqueue.spring.dao.utils.H2DatabaseInitializer;

public class CustomH2PickTaskDaoTest extends QueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        H2DatabaseInitializer.initialize();
    }

    public CustomH2PickTaskDaoTest() {
        super(
                new H2QueueDao(
                        H2DatabaseInitializer.getJdbcTemplate(),
                        H2DatabaseInitializer.CUSTOM_SCHEMA),
                (queueLocation, failureSettings) ->
                        new H2QueuePickTaskDao(
                                H2DatabaseInitializer.getJdbcTemplate(),
                                H2DatabaseInitializer.CUSTOM_SCHEMA,
                                queueLocation,
                                failureSettings),
                H2DatabaseInitializer.CUSTOM_TABLE_NAME,
                H2DatabaseInitializer.CUSTOM_SCHEMA,
                H2DatabaseInitializer.getJdbcTemplate(),
                H2DatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "now()";
    }

}
