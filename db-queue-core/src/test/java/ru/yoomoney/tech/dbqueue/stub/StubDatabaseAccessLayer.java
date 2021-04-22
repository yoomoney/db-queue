package ru.yoomoney.tech.dbqueue.stub;

import ru.yoomoney.tech.dbqueue.config.DatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.dao.PickTaskSettings;
import ru.yoomoney.tech.dbqueue.dao.QueueDao;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

public class StubDatabaseAccessLayer implements DatabaseAccessLayer {
    @Override
    public QueueDao getQueueDao() {
        return null;
    }

    @Override
    public QueuePickTaskDao createQueuePickTaskDao(@Nonnull PickTaskSettings pickTaskSettings) {
        return null;
    }

    @Override
    public <T> T transact(Supplier<T> supplier) {
        return null;
    }

    @Override
    public void transact(Runnable runnable) {

    }

    @Nonnull
    @Override
    public DatabaseDialect getDatabaseDialect() {
        return null;
    }

    @Nonnull
    @Override
    public QueueTableSchema getQueueTableSchema() {
        return null;
    }
}
