package ru.yandex.money.common.dbqueue.internal.dao;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class CustomPostgresQueuePickTaskDaoTest extends PostgresQueuePickTaskDaoTest {
    public CustomPostgresQueuePickTaskDaoTest() {
        super(TableSchemaType.CUSTOM);
    }
}
