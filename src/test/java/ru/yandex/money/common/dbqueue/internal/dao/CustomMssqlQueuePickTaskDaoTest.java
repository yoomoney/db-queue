package ru.yandex.money.common.dbqueue.internal.dao;

import org.junit.BeforeClass;

import ru.yandex.money.common.dbqueue.dao.BaseDaoTest;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

/**
 * @author Oleg Kandaurov
 * @author Behrooz Shabani
 * @since 25.01.2020
 */
public class CustomMssqlQueuePickTaskDaoTest extends MssqlQueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        QueueDatabaseInitializer.databaseType = QueueDatabaseInitializer.DatabaseType.MS;
        BaseDaoTest.beforeClass();
    }

    public CustomMssqlQueuePickTaskDaoTest() {
        super(TableSchemaType.MS_CUSTOM);
    }
}
