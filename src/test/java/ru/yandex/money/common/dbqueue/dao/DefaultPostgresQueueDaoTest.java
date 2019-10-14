package ru.yandex.money.common.dbqueue.dao;

/**
 * @author Oleg Kandaurov
 * @since 12.10.2019
 */
public class DefaultPostgresQueueDaoTest extends PostgresQueueDaoTest {

    public DefaultPostgresQueueDaoTest() {
        super(TableSchemaType.DEFAULT);
    }
}
