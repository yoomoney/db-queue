package ru.yandex.money.common.dbqueue.api.impl;

import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.ShardRouter;
import ru.yandex.money.common.dbqueue.dao.QueueDao;

import java.util.Collection;
import java.util.Collections;

/**
 * Реализация правил шардирования, в случае когда шардирование не требуется.
 *
 * @param <T> тип данныз задачи
 */
public class SingleShardRouter<T> implements ShardRouter<T> {
    private final QueueDao queueDao;

    /**
     * Конструктор.
     *
     * @param queueDao шард на котором происходит обработка задач
     */
    public SingleShardRouter(QueueDao queueDao) {
        this.queueDao = queueDao;
    }

    @Override
    public QueueShardId resolveShardId(EnqueueParams<T> enqueueParams) {
        return queueDao.getShardId();
    }

    @Override
    public Collection<QueueShardId> getShardsId() {
        return Collections.singletonList(queueDao.getShardId());
    }
}
