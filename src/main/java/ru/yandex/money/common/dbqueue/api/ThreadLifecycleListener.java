package ru.yandex.money.common.dbqueue.api;

import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;

/**
 * Слушатель хода выполнения потока очереди.
 *
 * @author Oleg Kandaurov
 * @since 16.07.2017
 */
public interface ThreadLifecycleListener {

    /**
     * Начало обработки задачи в очереди.
     * <p>
     * Вызывается всегда.
     * <p>
     * Может быть использовано для того чтобы проставить
     * значения в контекст логирования или поменять имя потока.
     *
     * @param shardId  идентификатор шарда на котором происходит обработка
     * @param location местоположение очереди
     */
    void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location);

    /**
     * Поток завершил полезную работу по обработке задачи.
     *
     * Вызывается в случае штатного завершения обработки.
     *
     * Может быть использовано для того, чтобы замерить изменение общей производительности обработки очередей.
     *
     * @param shardId идентификатор шарда на котором происходит обработка
     * @param location местоположение очереди
     * @param taskProcessed признак что задача была взята и обработана, в противном случае - не было задач на обработку.
     * @param threadBusyTime время нахождения потока в активном состоянии перед тем как заснуть.
     */
    void executed(QueueShardId shardId, QueueLocation location, boolean taskProcessed, long threadBusyTime);

    /**
     * Завершение цикла обработки задачи в очереди и старт нового цикла обработки.
     * <p>
     * Вызывается всегда, даже после {@link #crashed}
     * <p>
     * Может быть использовано для чтобы вернуть контекст логирования
     * или имя потока в прежнее состояние.
     *
     * @param shardId  идентификатор шарда на котором происходит обработка
     * @param location местоположение очереди
     */
    void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location);

    /**
     * Обработка очереди завершилась фатальной ошибкой.
     * <p>
     * Клиентский код по обработке задач не провоцирует вызов данного метода.
     * Вызывается в случае ошибок в механизме выборки задач.
     * <p>
     * Может быть использовано для логирования и мониторинга.
     *
     * @param shardId  идентификатор шарда на котором происходит обработка
     * @param location местоположение очереди
     * @param exc      исключение приведшее к неуспеху обработки
     */
    void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull Throwable exc);
}
