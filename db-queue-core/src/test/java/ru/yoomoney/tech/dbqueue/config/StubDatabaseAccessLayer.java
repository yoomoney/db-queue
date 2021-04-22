package ru.yoomoney.tech.dbqueue.config;

import ru.yoomoney.tech.dbqueue.dao.PickTaskSettings;
import ru.yoomoney.tech.dbqueue.dao.QueueDao;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;

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
    public QueueDao getQueueDao() {
        return queueDao;
    }

    @Override
    public QueuePickTaskDao createQueuePickTaskDao(@Nonnull PickTaskSettings pickTaskSettings) {
        return mock(QueuePickTaskDao.class);
    }

    @Override
    public <T> T transact(Supplier<T> supplier) {
        return supplier.get();
    }

    @Override
    public void transact(Runnable runnable) {
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
