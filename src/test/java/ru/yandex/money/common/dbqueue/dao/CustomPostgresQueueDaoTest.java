package ru.yandex.money.common.dbqueue.dao;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class CustomPostgresQueueDaoTest extends PostgresQueueDaoTest {

    public CustomPostgresQueueDaoTest() {
        super(TableSchemaType.CUSTOM);
    }
}
