package ru.yoomoney.tech.dbqueue.spring.dao;

import org.junit.BeforeClass;
import ru.yoomoney.tech.dbqueue.spring.dao.utils.H2DatabaseInitializer;

public class CustomH2QueueDaoTest extends QueueDaoTest {

    @BeforeClass
    public static void beforeClass() {
        H2DatabaseInitializer.initialize();
    }

    public CustomH2QueueDaoTest() {
        super(
                new H2QueueDao(H2DatabaseInitializer.getJdbcTemplate(), H2DatabaseInitializer.CUSTOM_SCHEMA),
                H2DatabaseInitializer.CUSTOM_TABLE_NAME,
                H2DatabaseInitializer.CUSTOM_SCHEMA,
                H2DatabaseInitializer.getJdbcTemplate(),
                H2DatabaseInitializer.getTransactionTemplate());
    }
}
