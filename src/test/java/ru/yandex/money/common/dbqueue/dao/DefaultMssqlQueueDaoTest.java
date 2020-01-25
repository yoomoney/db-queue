package ru.yandex.money.common.dbqueue.dao;

import org.junit.BeforeClass;

import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

/**
 * @author Oleg Kandaurov
 * @author Behrooz Shabani
 * @since 25.01.2020
 */
public class DefaultMssqlQueueDaoTest extends MssqlQueueDaoTest {

    @BeforeClass
    public static void beforeClass() {
        QueueDatabaseInitializer.databaseType = QueueDatabaseInitializer.DatabaseType.MS;
        BaseDaoTest.beforeClass();
    }

    public DefaultMssqlQueueDaoTest() {
        super(TableSchemaType.MS_DEFAULT);
    }
}
