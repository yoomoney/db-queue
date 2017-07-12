package ru.yandex.money.common.dbqueue.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * Сырые данные задачи, выбранные из БД.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public class TaskRecord {
    private final long id;
    @Nullable
    private final String payload;
    private final long attemptsCount;
    @Nonnull
    private final ZonedDateTime createDate;
    @Nonnull
    private final ZonedDateTime processTime;
    @Nullable
    private final String correlationId;
    @Nullable
    private final String actor;

    /**
     * Конструктор
     *
     * @param id идентификатор (sequence id) задачи
     * @param payload сырые данные задачи
     * @param attemptsCount количество попыток исполнения задачи
     * @param createDate дата постановки задачи
     * @param processTime время очередной обработки задачи
     * @param correlationId технический идентификатор
     * @param actor бизнесовый идентификатор
     */
    public TaskRecord(long id, @Nullable String payload, long attemptsCount, @Nonnull ZonedDateTime createDate,
                      @Nonnull ZonedDateTime processTime, @Nullable String correlationId, @Nullable String actor) {
        this.id = id;
        this.payload = payload;
        this.attemptsCount = attemptsCount;
        this.createDate = createDate;
        this.processTime = processTime;
        this.correlationId = correlationId;
        this.actor = actor;
    }

    /**
     * Получить идентификатор (sequence id) задачи
     * @return идентификатор задачи
     */
    public long getId() {
        return id;
    }

    /**
     * Получить сырые данные задачи
     * @return данные задачи
     */
    @Nullable
    public String getPayload() {
        return payload;
    }

    /**
     * Получить количество попыток исполнения задачи, включая текущую.
     *
     * @return количество попыток исполнения
     */
    public long getAttemptsCount() {
        return attemptsCount;
    }

    /**
     * Получить дату постановки задачи в очередь
     *
     * @return дата постановки задачи
     */
    @Nonnull
    public ZonedDateTime getCreateDate() {
        return createDate;
    }

    /**
     * Получить технический идентификатор задачи
     *
     * @return технический идентификатор
     */
    @Nullable
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * Получить время следущей обработки задачи
     *
     * @return время обработки
     */
    @Nonnull
    public ZonedDateTime getProcessTime() {
        return processTime;
    }

    /**
     * Получить бизнесовый идентификатор задачи.
     *
     * @return идентификатор задачи.
     */
    @Nullable
    public String getActor() {
        return actor;
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
                Objects.equals(payload, that.payload) &&
                Objects.equals(createDate, that.createDate) &&
                Objects.equals(processTime, that.processTime) &&
                Objects.equals(correlationId, that.correlationId) &&
                Objects.equals(actor, that.actor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, payload, attemptsCount, createDate, processTime, correlationId, actor);
    }

    @Override
    public String toString() {
        return '{' +
                "id=" + id +
                ", attemptsCount=" + attemptsCount +
                ", createDate=" + createDate +
                ", processTime=" + processTime +
                (correlationId != null ? ", correlationId=" + correlationId : "") +
                (actor != null ? ", actor=" + actor : "") +
                '}';
    }
}
