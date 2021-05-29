package ru.yoomoney.tech.dbqueue.spring.dao;

import org.junit.BeforeClass;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.spring.dao.utils.H2DatabaseInitializer;

import java.util.UUID;


public class DefaultH2WithSequenceQueueDaoTest extends QueueDaoTest {

    @BeforeClass
    public static void beforeClass() {
        H2DatabaseInitializer.initialize();
    }

    public DefaultH2WithSequenceQueueDaoTest() {
        super(
                new H2QueueDao(
                        H2DatabaseInitializer.getJdbcTemplate(),
                        H2DatabaseInitializer.DEFAULT_SCHEMA),
                H2DatabaseInitializer.DEFAULT_TABLE_NAME_WO_INC,
                H2DatabaseInitializer.DEFAULT_SCHEMA,
                H2DatabaseInitializer.getJdbcTemplate(),
                H2DatabaseInitializer.getTransactionTemplate());
    }

    protected QueueLocation generateUniqueLocation() {
        return QueueLocation.builder().withTableName(tableName)
                .withQueueId(new QueueId("test-queue-" + UUID.randomUUID()))
                .withIdSequence("tasks_seq").build();
    }
}