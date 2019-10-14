package ru.yandex.money.common.dbqueue.internal.dao;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class DefaultPostgresQueuePickTaskDaoTest extends PostgresQueuePickTaskDaoTest {
    public DefaultPostgresQueuePickTaskDaoTest() {
        super(TableSchemaType.DEFAULT);
    }
}
