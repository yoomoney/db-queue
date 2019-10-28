package ru.yandex.money.common.dbqueue.dao;

import org.junit.BeforeClass;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yandex.money.common.dbqueue.config.QueueTableSchema;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * @author Oleg Kandaurov
 * @since 12.07.2017
 */
public class BaseDaoTest {

    public enum TableSchemaType {
        DEFAULT, CUSTOM
    }

    private static final AtomicLong queueCounter = new AtomicLong();


    protected static JdbcTemplate jdbcTemplate;
    private static TransactionTemplate transactionTemplate;

    protected final String tableName;
    protected final QueueTableSchema tableSchema;

    public BaseDaoTest(TableSchemaType tableSchemaType) {
        switch (tableSchemaType) {
            case CUSTOM:
                tableName = QueueDatabaseInitializer.CUSTOM_TABLE_NAME;
                tableSchema = QueueDatabaseInitializer.CUSTOM_SCHEMA;
                return;
            case DEFAULT:
                tableName = QueueDatabaseInitializer.DEFAULT_TABLE_NAME;
                tableSchema = QueueDatabaseInitializer.DEFAULT_SCHEMA;
                return;
            default:
                throw new IllegalArgumentException();
        }
    }

    @BeforeClass
    public static void beforeClass() {
        jdbcTemplate = QueueDatabaseInitializer.getJdbcTemplate();
        transactionTemplate = QueueDatabaseInitializer.getTransactionTemplate();
    }

    protected QueueLocation generateUniqueLocation() {
        return QueueLocation.builder().withTableName(tableName)
                .withQueueId(new QueueId("test-queue-" + queueCounter.incrementAndGet())).build();
    }

    protected void executeInTransaction(Runnable runnable) {
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                runnable.run();
            }
        });
    }

    protected <T> T executeInTransaction(Supplier<T> supplier) {
        return transactionTemplate.execute(status -> supplier.get());
    }
}
