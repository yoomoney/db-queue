package ru.yandex.money.common.dbqueue.settings;

/**
 * Стратегия по которой производится откладывание задачи в случае повтора.
 *
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public enum TaskRetryType {

    /**
     * Задача откладывается по геометрической прогрессии в минутах.
     * Знаменатель прогрессии равен двум.
     * Первые 6 членов: 1 2 4 8 16 32
     */
    GEOMETRIC_BACKOFF,
    /**
     * Задача откладывается по арифметической прогрессии в минутах.
     * Разность прогрессии равна двум.
     * Первые 6 членов: 1 3 5 7 9 11
     */
    ARITHMETIC_BACKOFF,
    /**
     * Задача откладывает с фиксированной задержкой.
     * <p>
     * Значение фиксированной задержки устанавливается через настройку
     * {@link QueueSettings.AdditionalSetting#RETRY_FIXED_INTERVAL_DELAY}
     */
    FIXED_INTERVAL
}
