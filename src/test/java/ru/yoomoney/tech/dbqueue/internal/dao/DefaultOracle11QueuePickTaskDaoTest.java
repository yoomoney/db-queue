package ru.yoomoney.tech.dbqueue.internal.dao;

import org.junit.BeforeClass;
import org.junit.Ignore;
import ru.yoomoney.tech.dbqueue.dao.Oracle11QueueDao;
import ru.yoomoney.tech.dbqueue.internal.pick.Oracle11QueuePickTaskDao;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.utils.OracleDatabaseInitializer;
import ru.yoomoney.tech.dbqueue.utils.PostgresDatabaseInitializer;

import java.util.UUID;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
@Ignore("https://github.com/yoomoney-tech/db-queue/issues/10")
public class DefaultOracle11QueuePickTaskDaoTest extends QueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        OracleDatabaseInitializer.initialize();
    }

    public DefaultOracle11QueuePickTaskDaoTest() {
        super(new Oracle11QueueDao(OracleDatabaseInitializer.getJdbcTemplate(), OracleDatabaseInitializer.DEFAULT_SCHEMA),
                pickTaskSettings -> new Oracle11QueuePickTaskDao(OracleDatabaseInitializer.getJdbcTemplate(),
                        PostgresDatabaseInitializer.DEFAULT_SCHEMA, pickTaskSettings),
                OracleDatabaseInitializer.DEFAULT_TABLE_NAME, OracleDatabaseInitializer.DEFAULT_SCHEMA,
                OracleDatabaseInitializer.getJdbcTemplate(), OracleDatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "CURRENT_TIMESTAMP";
    }

    @Override
    protected QueueLocation generateUniqueLocation() {
        return QueueLocation.builder().withTableName(tableName)
                .withIdSequence("tasks_seq")
                .withQueueId(new QueueId("test-queue-" + UUID.randomUUID())).build();
    }
}
