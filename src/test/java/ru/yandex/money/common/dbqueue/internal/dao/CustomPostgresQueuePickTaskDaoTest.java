package ru.yandex.money.common.dbqueue.internal.dao;

import org.junit.BeforeClass;

import ru.yandex.money.common.dbqueue.dao.BaseDaoTest;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class CustomPostgresQueuePickTaskDaoTest extends PostgresQueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        QueueDatabaseInitializer.databaseType = QueueDatabaseInitializer.DatabaseType.PG;
        BaseDaoTest.beforeClass();
    }

    public CustomPostgresQueuePickTaskDaoTest() {
        super(TableSchemaType.PG_CUSTOM);
    }
}
