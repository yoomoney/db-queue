package ru.yoomoney.tech.dbqueue.spring.dao;

import org.junit.BeforeClass;
import org.junit.Ignore;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.spring.dao.utils.OracleDatabaseInitializer;

import java.util.UUID;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
@Ignore("https://github.com/yoomoney/db-queue/issues/10")
public class CustomOracle11QueueDaoTest extends QueueDaoTest {

    @BeforeClass
    public static void beforeClass() {
        OracleDatabaseInitializer.initialize();
    }

    public CustomOracle11QueueDaoTest() {
        super(new Oracle11QueueDao(OracleDatabaseInitializer.getJdbcTemplate(), OracleDatabaseInitializer.CUSTOM_SCHEMA),
                OracleDatabaseInitializer.CUSTOM_TABLE_NAME, OracleDatabaseInitializer.CUSTOM_SCHEMA,
                OracleDatabaseInitializer.getJdbcTemplate(), OracleDatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected QueueLocation generateUniqueLocation() {
        return QueueLocation.builder().withTableName(tableName)
                .withIdSequence("tasks_seq")
                .withQueueId(new QueueId("test-queue-" + UUID.randomUUID())).build();
    }
}
