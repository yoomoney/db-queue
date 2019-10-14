package ru.yandex.money.common.dbqueue.api;

import ru.yandex.money.common.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.Executor;

/**
 * Обработчик задач в очереди.
 *
 * @param <T> Тип данных задачи в очереди
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public interface QueueConsumer<T> {

    /**
     * Обработать задачу.
     *
     * @param task задача на обработку
     * @return результат выполнения очереди
     */
    @Nonnull
    TaskExecutionResult execute(@Nonnull Task<T> task);

    /**
     * Получить конфигурацию данной очереди.
     *
     * @return конфигурация очереди
     */
    @Nonnull
    QueueConfig getQueueConfig();

    /**
     * Предоставить преобразователь данных задачи.
     *
     * @return преобразователь данных
     */
    @Nonnull
    TaskPayloadTransformer<T> getPayloadTransformer();

    /**
     * Исполнитель очереди в режиме {@link ru.yandex.money.common.dbqueue.settings.ProcessingMode#USE_EXTERNAL_EXECUTOR}.
     * Используется только для вышеназванного режима.
     *
     * @return опциональный обработчик задач в очереди
     */
    default Optional<Executor> getExecutor() {
        return Optional.empty();
    }

}
