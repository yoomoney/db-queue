package ru.yandex.money.common.dbqueue.api;

import ru.yandex.money.common.dbqueue.config.QueueShardId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Параметры типизированной задачи, поставляемые в обработчик очереди.
 *
 * @param <T> тип данных задачи
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public final class Task<T> {

    @Nonnull
    private final QueueShardId shardId;
    @Nullable
    private final T payload;
    private final long attemptsCount;
    private final long reenqueueAttemptsCount;
    private final long totalAttemptsCount;
    @Nonnull
    private final ZonedDateTime createdAt;
    @Nonnull
    private final Map<String, String> extData;

    /**
     * Конструктор типизированных параметров задачи
     *
     * @param shardId                идентификатор шарда, с которого была взята задача
     * @param payload                данные задачи
     * @param attemptsCount          количество попыток выполнения, включая текущую
     * @param reenqueueAttemptsCount количество попыток переоткладывания задачи
     * @param totalAttemptsCount     суммарное количество попыток выполнить задачу, включая все попытки переоткладывания
     *                               и все неудачные попытки
     * @param createdAt             время помещения задачи в очередь
     * @param extData                расширенные данные задачи, ключ - это имя колонки БД
     */
    private Task(@Nonnull QueueShardId shardId, @Nullable T payload,
                 long attemptsCount, long reenqueueAttemptsCount, long totalAttemptsCount,
                 @Nonnull ZonedDateTime createdAt, @Nonnull Map<String, String> extData) {
        this.shardId = requireNonNull(shardId, "shardId");
        this.payload = payload;
        this.attemptsCount = attemptsCount;
        this.reenqueueAttemptsCount = reenqueueAttemptsCount;
        this.totalAttemptsCount = totalAttemptsCount;
        this.createdAt = requireNonNull(createdAt, "createdAt");
        this.extData = requireNonNull(extData, "extData");
    }

    /**
     * Получить типизированные данные задачи
     *
     * @return типизированные данные задачи
     */
    @Nonnull
    public Optional<T> getPayload() {
        return Optional.ofNullable(payload);
    }

    /**
     * Получить типизированные данные задачи или выбросить исключение
     * при их отсутствии.
     *
     * @return типизированные данные задачи
     */
    @Nonnull
    public T getPayloadOrThrow() {
        if (payload == null) {
            throw new IllegalArgumentException("payload is absent");
        }
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
     * Получить идентификатор шарда, с которого была взята задача
     *
     * @return идентификатор шарда
     */
    @Nonnull
    public QueueShardId getShardId() {
        return shardId;
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
        Task<?> task = (Task<?>) obj;
        return attemptsCount == task.attemptsCount &&
                reenqueueAttemptsCount == task.reenqueueAttemptsCount &&
                totalAttemptsCount == task.totalAttemptsCount &&
                Objects.equals(shardId, task.shardId) &&
                Objects.equals(payload, task.payload) &&
                Objects.equals(createdAt, task.createdAt) &&
                Objects.equals(extData, task.extData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, payload, attemptsCount, reenqueueAttemptsCount,
                totalAttemptsCount, createdAt, extData);
    }

    @Override
    public String toString() {
        return '{' +
                "shardId=" + shardId +
                ", attemptsCount=" + attemptsCount +
                ", reenqueueAttemptsCount=" + reenqueueAttemptsCount +
                ", totalAttemptsCount=" + totalAttemptsCount +
                ", createdAt=" + createdAt +
                ", payload=" + payload +
                '}';
    }

    /**
     * Creates builder for {@link Task} objects.
     *
     * @param shardId an id of shard
     * @param <T> a type of task payload
     * @return new instance of {@link Builder}
     */
    public static <T> Builder<T> builder(@Nonnull QueueShardId shardId) {
        return new Builder<>(shardId);
    }

    /**
     * Билдер для класса {@link Task}
     *
     * @param <T> тип данных задачи
     */
    public static class Builder<T> {
        @Nonnull
        private final QueueShardId shardId;
        @Nonnull
        private ZonedDateTime createdAt = ZonedDateTime.now();
        private T payload;
        private long attemptsCount;
        private long reenqueueAttemptsCount;
        private long totalAttemptsCount;
        @Nonnull
        private Map<String, String> extData = new LinkedHashMap<>();

        private Builder(@Nonnull QueueShardId shardId) {
            this.shardId = requireNonNull(shardId, "shardId");
        }

        public Builder<T> withCreatedAt(@Nonnull ZonedDateTime createdAt) {
            this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
            return this;
        }

        public Builder<T> withPayload(T payload) {
            this.payload = payload;
            return this;
        }

        public Builder<T> withAttemptsCount(long attemptsCount) {
            this.attemptsCount = attemptsCount;
            return this;
        }

        public Builder<T> withReenqueueAttemptsCount(long reenqueueAttemptsCount) {
            this.reenqueueAttemptsCount = reenqueueAttemptsCount;
            return this;
        }

        public Builder<T> withTotalAttemptsCount(long totalAttemptsCount) {
            this.totalAttemptsCount = totalAttemptsCount;
            return this;
        }

        public Builder<T> withExtData(@Nonnull Map<String, String> extData) {
            this.extData = requireNonNull(extData);
            return this;
        }

        public Task<T> build() {
            return new Task<>(shardId, payload, attemptsCount, reenqueueAttemptsCount,
                    totalAttemptsCount, createdAt, extData);
        }
    }
}
