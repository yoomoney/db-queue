package ru.yandex.money.common.dbqueue.dao;

import org.junit.BeforeClass;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * @author Oleg Kandaurov
 * @since 12.07.2017
 */
public class BaseDaoTest {

    private static final AtomicLong queueCounter = new AtomicLong();


    protected static JdbcTemplate jdbcTemplate;
    protected static TransactionTemplate transactionTemplate;


    @BeforeClass
    public static void beforeClass() {
        jdbcTemplate = QueueDatabaseInitializer.getJdbcTemplate();
        transactionTemplate = QueueDatabaseInitializer.getTransactionTemplate();
    }

    protected static QueueLocation generateUniqueLocation() {
        return QueueLocation.builder().withTableName(QueueDatabaseInitializer.DEFAULT_TABLE_NAME)
                .withQueueName("test-queue-" + queueCounter.incrementAndGet()).build();
    }

    protected static String generateUniqueTable() {
        return QueueDatabaseInitializer.DEFAULT_TABLE_NAME + "_" + queueCounter.incrementAndGet();
    }

    protected static void executeInTransaction(Runnable runnable) {
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                runnable.run();
            }
        });
    }

    protected static <T> T executeInTransaction(Supplier<T> supplier) {
        return transactionTemplate.execute(new TransactionCallback<T>() {
            @Override
            public T doInTransaction(TransactionStatus status) {
                return supplier.get();
            }
        });
    }
}
