package ru.yandex.money.common.dbqueue.settings;

/**
 * Стратегия по которой производится откладывание задачи в случае повтора.
 *
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public enum TaskRetryType {

    /**
     * Задача откладывается по геометрической прогрессии относительно интервала
     * {@link QueueSettings#getRetryInterval()}
     * Знаменатель прогрессии равен двум.
     * Первые 6 членов: 1 2 4 8 16 32
     */
    GEOMETRIC_BACKOFF,
    /**
     * Задача откладывается по арифметической прогрессии относительно интервала
     * {@link QueueSettings#getRetryInterval()}.
     * Разность прогрессии равна двум.
     * Первые 6 членов: 1 3 5 7 9 11
     */
    ARITHMETIC_BACKOFF,
    /**
     * Задача откладывает с фиксированной задержкой.
     * <p>
     * Значение фиксированной задержки устанавливается через настройку
     * {@link QueueSettings#getRetryInterval()}
     */
    LINEAR_BACKOFF
}
