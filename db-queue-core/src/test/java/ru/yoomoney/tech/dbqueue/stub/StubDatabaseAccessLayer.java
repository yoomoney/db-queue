package ru.yoomoney.tech.dbqueue.stub;

import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.dao.QueueDao;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;
import ru.yoomoney.tech.dbqueue.settings.FailureSettings;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;

public class StubDatabaseAccessLayer implements DatabaseAccessLayer {

    private final QueueDao queueDao;

    public StubDatabaseAccessLayer() {
        this.queueDao = mock(QueueDao.class);
    }

    public StubDatabaseAccessLayer(QueueDao queueDao) {
        this.queueDao = queueDao;
    }

    @Override
    @Nonnull
    public QueueDao getQueueDao() {
        return queueDao;
    }

    @Override
    @Nonnull
    public QueuePickTaskDao createQueuePickTaskDao(@Nonnull QueueLocation queueLocation,
                                                   @Nonnull FailureSettings failureSettings) {
        return mock(QueuePickTaskDao.class);
    }

    @Override
    public <T> T transact(@Nonnull Supplier<T> supplier) {
        return supplier.get();
    }

    @Override
    public void transact(@Nonnull Runnable runnable) {
        runnable.run();
    }

    @Nonnull
    @Override
    public DatabaseDialect getDatabaseDialect() {
        return DatabaseDialect.POSTGRESQL;
    }

    @Nonnull
    @Override
    public QueueTableSchema getQueueTableSchema() {
        return QueueTableSchema.builder().build();
    }
}
