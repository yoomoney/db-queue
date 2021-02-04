package ru.yoomoney.tech.dbqueue.dao;

import org.junit.BeforeClass;
import org.junit.Ignore;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.utils.OracleDatabaseInitializer;

import java.util.UUID;

/**
 * @author Oleg Kandaurov
 * @since 15.05.2020
 */
@Ignore("https://github.com/yoomoney-tech/db-queue/issues/10")
public class DefaultOracle11QueueDaoTest extends QueueDaoTest {

    @BeforeClass
    public static void beforeClass() {
        OracleDatabaseInitializer.initialize();
    }

    public DefaultOracle11QueueDaoTest() {
        super(new Oracle11QueueDao(OracleDatabaseInitializer.getJdbcTemplate(), OracleDatabaseInitializer.DEFAULT_SCHEMA),
                OracleDatabaseInitializer.DEFAULT_TABLE_NAME, OracleDatabaseInitializer.DEFAULT_SCHEMA,
                OracleDatabaseInitializer.getJdbcTemplate(), OracleDatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected QueueLocation generateUniqueLocation() {
        return QueueLocation.builder().withTableName(tableName)
                .withIdSequence("tasks_seq")
                .withQueueId(new QueueId("test-queue-" + UUID.randomUUID())).build();
    }
}
