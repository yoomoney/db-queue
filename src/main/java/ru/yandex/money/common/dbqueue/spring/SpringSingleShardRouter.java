package ru.yandex.money.common.dbqueue.spring;

import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.impl.SingleShardRouter;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import java.util.Collection;
import java.util.Collections;

/**
 * Класс, описывающие правила шардирования, когда таковое не требуется
 * и доступен только один шард.
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 05.08.2017
 */
public class SpringSingleShardRouter<T> extends SpringShardRouter<T> {

    private final QueueDao queueDao;
    private SingleShardRouter<T> singleShardRouter;

    /**
     * Конструктор
     *
     * @param queueLocation местоположение очереди
     * @param payloadClass  класс данных задачи
     * @param queueDao      шард на котором размещены задачи
     */
    public SpringSingleShardRouter(QueueLocation queueLocation, Class<T> payloadClass, QueueDao queueDao) {
        super(queueLocation, payloadClass);
        this.queueDao = queueDao;
    }

    @Override
    public QueueShardId resolveShardId(EnqueueParams<T> enqueueParams) {
        if (singleShardRouter == null) {
            singleShardRouter = new SingleShardRouter<>(queueDao);
        }
        return singleShardRouter.resolveShardId(enqueueParams);
    }

    @Override
    public Collection<QueueShardId> getShardsId() {
        return Collections.singletonList(queueDao.getShardId());
    }
}
