package ru.yandex.money.common.dbqueue.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Сырые данные задачи, выбранные из БД.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public final class TaskRecord {
    private final long id;
    @Nullable
    private final String payload;
    private final long attemptsCount;
    private final long reenqueueAttemptsCount;
    private final long totalAttemptsCount;
    @Nonnull
    private final ZonedDateTime createdAt;
    @Nonnull
    private final ZonedDateTime nextProcessAt;
    @Nonnull
    private final Map<String, String> extData;

    /**
     * Конструктор
     *
     * @param id                     идентификатор (sequence id) задачи
     * @param payload                сырые данные задачи
     * @param attemptsCount          количество попыток исполнения задачи
     * @param reenqueueAttemptsCount количество попыток переоткладывания задачи
     * @param totalAttemptsCount     суммарное количество попыток выполнить задачу
     * @param createdAt             время постановки задачи
     * @param nextProcessAt            время очередной обработки задачи
     * @param extData                расширенные данные задачи, ключ - это имя колонки БД
     */
    private TaskRecord(long id,
                       @Nullable String payload,
                       long attemptsCount,
                       long reenqueueAttemptsCount,
                       long totalAttemptsCount,
                       @Nonnull ZonedDateTime createdAt,
                       @Nonnull ZonedDateTime nextProcessAt,
                       @Nonnull Map<String, String> extData) {
        this.id = id;
        this.payload = payload;
        this.attemptsCount = attemptsCount;
        this.reenqueueAttemptsCount = reenqueueAttemptsCount;
        this.totalAttemptsCount = totalAttemptsCount;
        this.createdAt = Objects.requireNonNull(createdAt);
        this.nextProcessAt = Objects.requireNonNull(nextProcessAt);
        this.extData = Objects.requireNonNull(extData);
    }

    /**
     * Получить идентификатор (sequence id) задачи
     *
     * @return идентификатор задачи
     */
    public long getId() {
        return id;
    }

    /**
     * Получить сырые данные задачи
     *
     * @return данные задачи
     */
    @Nullable
    public String getPayload() {
        return payload;
    }

    /**
     * Получить количество попыток исполнения задачи после последнего re-enqueue, включая текущую.
     *
     * @return количество попыток исполнения
     */
    public long getAttemptsCount() {
        return attemptsCount;
    }

    /**
     * Получить количество попыток переоткладывания задачи.
     *
     * @return количество попыток переоткладывания
     */
    public long getReenqueueAttemptsCount() {
        return reenqueueAttemptsCount;
    }

    /**
     * Получить суммарное количество попыток выполнить задачу.
     * Этот счетчик учитывает все попытки, включая неуспешные и с возвратом в очередь (re-enqueue),
     * и никогда не сбрасывается.
     *
     * @return суммарное количество попыток выполнения
     */
    public long getTotalAttemptsCount() {
        return totalAttemptsCount;
    }

    /**
     * Получить время постановки задачи в очередь
     *
     * @return время постановки задачи
     */
    @Nonnull
    public ZonedDateTime getCreatedAt() {
        return createdAt;
    }

    /**
     * Получить время следущей обработки задачи
     *
     * @return время обработки
     */
    @Nonnull
    public ZonedDateTime getNextProcessAt() {
        return nextProcessAt;
    }

    /**
     * Получить расширенный набор данных задачи
     *
     * @return дополнительные данные задачи, в ключе содержится имя колонки в БД
     */
    @Nonnull
    public Map<String, String> getExtData() {
        return extData;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TaskRecord that = (TaskRecord) obj;
        return id == that.id &&
                attemptsCount == that.attemptsCount &&
                reenqueueAttemptsCount == that.reenqueueAttemptsCount &&
                totalAttemptsCount == that.totalAttemptsCount &&
                Objects.equals(payload, that.payload) &&
                Objects.equals(createdAt, that.createdAt) &&
                Objects.equals(nextProcessAt, that.nextProcessAt) &&
                Objects.equals(extData, that.extData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, payload, attemptsCount, reenqueueAttemptsCount, totalAttemptsCount,
                createdAt, nextProcessAt, extData);
    }

    @Override
    public String toString() {
        return '{' +
                "id=" + id +
                ", attemptsCount=" + attemptsCount +
                ", reenqueueAttemptsCount=" + reenqueueAttemptsCount +
                ", totalAttemptsCount=" + totalAttemptsCount +
                ", createdAt=" + createdAt +
                ", nextProcessAt=" + nextProcessAt +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Билдер для класса {@link TaskRecord}
     */
    public static class Builder {
        private long id;
        @Nullable
        private String payload;
        private long attemptsCount;
        private long reenqueueAttemptsCount;
        private long totalAttemptsCount;
        @Nonnull
        private ZonedDateTime createdAt = ZonedDateTime.now();
        @Nonnull
        private ZonedDateTime nextProcessAt = ZonedDateTime.now();
        @Nonnull
        private Map<String, String> extData = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder withCreatedAt(@Nonnull ZonedDateTime createdAt) {
            this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
            return this;
        }

        public Builder withNextProcessAt(@Nonnull ZonedDateTime nextProcessAt) {
            this.nextProcessAt = Objects.requireNonNull(nextProcessAt, "nextProcessAt");
            return this;
        }

        public Builder withId(long id) {
            this.id = id;
            return this;
        }

        public Builder withPayload(String payload) {
            this.payload = payload;
            return this;
        }

        public Builder withAttemptsCount(long attemptsCount) {
            this.attemptsCount = attemptsCount;
            return this;
        }

        public Builder withReenqueueAttemptsCount(long reenqueueAttemptsCount) {
            this.reenqueueAttemptsCount = reenqueueAttemptsCount;
            return this;
        }

        public Builder withTotalAttemptsCount(long totalAttemptsCount) {
            this.totalAttemptsCount = totalAttemptsCount;
            return this;
        }

        public Builder withExtData(@Nonnull Map<String, String> extData) {
            this.extData = Objects.requireNonNull(extData);
            return this;
        }

        public TaskRecord build() {
            return new TaskRecord(id, payload, attemptsCount, reenqueueAttemptsCount,
                    totalAttemptsCount, createdAt, nextProcessAt, extData);
        }
    }
}
