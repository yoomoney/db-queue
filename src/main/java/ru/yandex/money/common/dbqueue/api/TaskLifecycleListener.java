package ru.yandex.money.common.dbqueue.api;

import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;

/**
 * Слушатель хода обработки задачи в очереди
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public interface TaskLifecycleListener {

    /**
     * Событие выборки задачи из очереди.
     * <p>
     * Вызывается если в очереди есть задача, доступная для обработки.
     * <p>
     * Может быть использовано для разбора проблем с производительностью БД.
     *
     * @param shardId      идентификатор шарда на котором происходит обработка
     * @param location     местоположение очереди
     * @param taskRecord   данные задачи
     * @param pickTaskTime время выборки задачи
     */
    void picked(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord,
                long pickTaskTime);

    /**
     * Событие начала обработки задачи.
     * <p>
     * Вызывается всегда, если задача была выбрана.
     * <p>
     * Может быть использовано для выставления записи в контекст логирования.
     *
     * @param shardId    идентификатор шарда на котором происходит обработка
     * @param location   местоположение очереди
     * @param taskRecord данные задачи
     */
    void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord);

    /**
     * Событие завершения выполнения клиентской логики в очереди.
     * <p>
     * Вызывается в случае, когда задача завершила работу штатно.
     * <p>
     * Может быть использовано для различного рода мониторинга
     * успеха выполнения бизнес логики.
     *
     * @param shardId         идентификатор шарда на котором происходит обработка
     * @param location        местоположение очереди
     * @param taskRecord      данные задачи
     * @param executionResult результат выполнения задачи
     * @param processTaskTime время обработки задачи, не включая время выборки
     */
    void executed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord,
                  @Nonnull TaskExecutionResult executionResult, long processTaskTime);

    /**
     * Событие завершение обработки задачи в очереди.
     * <p>
     * Вызывается всегда, если задача была взята на обработку.
     * Вызов происходит даже после {@link #crashed}
     * <p>
     * Может быть использовано для восстановления исходного состояния контекста логирования.
     *
     * @param shardId    идентификатор шарда на котором происходит обработка
     * @param location   местоположение очереди
     * @param taskRecord данные задачи
     */
    void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord);


    /**
     * Событие нештатной обработки очереди.
     * <p>
     * Вызывается в случае непредвиденной ошибки при выполнении очереди.
     * <p>
     * Может быть использовано для отслеживания ошибок в работе системы.
     *
     * @param shardId    идентификатор шарда на котором происходит обработка
     * @param location   местоположение очереди
     * @param taskRecord данные задачи
     * @param exc        исключение, вызвавшее ошибку
     */
    void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord,
                 @Nonnull Exception exc);

}
