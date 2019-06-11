package ru.yandex.money.common.dbqueue.settings;

import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskRecord;

import java.time.Duration;

/**
 * Тип стратегии, которая вычисляет задержку перед следующим выполнением задачи в случае, если задачу
 * {@link TaskExecutionResult.Type#REENQUEUE требуется вернуть в очередь}.
 *
 * @author Dmitry Komarov
 * @since 21.05.2019
 */
public enum ReenqueueRetryType {

    /**
     * Задача откладывается на время, задаваемое вручную с помощью вызова
     * {@link TaskExecutionResult#reenqueue(Duration)}.
     * <p>
     * Является значением по-умолчанию для стратегии переоткладывания задачи.
     * <p>
     * Пример настроек
     * <pre>
     * {@code db-queue.queueName.reenqueue-retry-type=manual}
     * </pre>
     */
    MANUAL,

    /**
     * Задача откладывается на время, задаваемое с помощью последовательности задержек. Задержка выбирается из
     * последовательности согласно {@link TaskRecord#getReenqueueAttemptsCount() номеру попытки переотложить задачу}.
     * Если номер попытки превышает размер заданной последовательности, будет выбран последний ее член.
     * <p>
     * Например, пусть в настройках задана следующая последовательность:
     * <pre>
     * {@code
     * db-queue.queueName.reenqueue-retry-type=sequential
     * db-queue.queueName.reenqueue-retry-plan=PT1S,PT10S,PT1M,P7D}
     * </pre>
     * Для первой попытки переотложить задачу будет выбрана задержка в размере 1 секунды ({@code PT1S}),
     * для второй - 10 секунд и т.д. Для пятой попытки и всех последующих - задержка составляет 7 дней.
     */
    SEQUENTIAL,

    /**
     * Задача откладывается на фиксированное время, заданное в конфигурации.
     * <p>
     * Пример настроек
     * <pre>
     * {@code
     * db-queue.queueName.reenqueue-retry-type=fixed
     * db-queue.queueName.reenqueue-retry-delay=PT10S}
     * </pre>
     * То есть для каждой попытки задача будет отложена на 10 секунд.
     */
    FIXED,

    /**
     * Задача откладывается на время, задаваемое с помощью арифметической прогрессии. Член прогрессии выбирается
     * согласно {@link TaskRecord#getReenqueueAttemptsCount() номеру попытки переотложить задачу}.
     * <p>
     * Прогрессия задается парой значений: первым членом ({@code reenqueue-retry-initial-delay})
     * и разностью ({@code reenqueue-retry-step}).
     * <pre>
     * {@code
     * db-queue.queueName.reenqueue-retry-type=arithmetic
     * db-queue.queueName.reenqueue-retry-initial-delay=PT1S
     * db-queue.queueName.reenqueue-retry-step=PT2S}
     * </pre>
     * Таким образом задача будет откладываться со следующими задержками: 1c, 3c, 5c, 7c, ...
     */
    ARITHMETIC,

    /**
     * Задача откладывается на время, задаваемое с помощью геометрической прогрессии. Член прогрессии выбирается
     * согласно {@link TaskRecord#getReenqueueAttemptsCount() номеру попытки переотложить задачу}.
     * <p>
     * Прогрессия задается парой значений: первым членом и целочисленным знаменателем.
     * <pre>
     * {@code
     * db-queue.queueName.reenqueue-retry-type=geometric
     * db-queue.queueName.reenqueue-retry-initial-delay=PT1S
     * db-queue.queueName.reenqueue-retry-ratio=2}
     * </pre>
     * Таким образом задача будет откладываться со следующими задержками: 1с, 2с, 4с, 8с, ...
     */
    GEOMETRIC
}
