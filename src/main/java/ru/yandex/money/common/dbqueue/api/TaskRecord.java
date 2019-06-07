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
public final class TaskRecord {
    private final long id;
    @Nullable
    private final String payload;
    private final long attemptsCount;
    private final long reenqueueAttemptsCount;
    private final long totalAttemptsCount;
    @Nonnull
    private final ZonedDateTime createDate;
    @Nonnull
    private final ZonedDateTime processTime;
    @Nullable
    private final String traceInfo;
    @Nullable
    private final String actor;

    /**
     * Конструктор
     *
     * @param id                     идентификатор (sequence id) задачи
     * @param payload                сырые данные задачи
     * @param attemptsCount          количество попыток исполнения задачи
     * @param reenqueueAttemptsCount количество попыток переоткладывания задачи
     * @param totalAttemptsCount     суммарное количество попыток выполнить задачу
     * @param createDate             дата постановки задачи
     * @param processTime            время очередной обработки задачи
     * @param traceInfo              данные трассировки
     * @param actor                  бизнесовый идентификатор
     */
    public TaskRecord(long id,
                      @Nullable String payload,
                      long attemptsCount,
                      long reenqueueAttemptsCount,
                      long totalAttemptsCount,
                      @Nonnull ZonedDateTime createDate,
                      @Nonnull ZonedDateTime processTime,
                      @Nullable String traceInfo,
                      @Nullable String actor) {
        this.id = id;
        this.payload = payload;
        this.attemptsCount = attemptsCount;
        this.reenqueueAttemptsCount = reenqueueAttemptsCount;
        this.totalAttemptsCount = totalAttemptsCount;
        this.createDate = createDate;
        this.processTime = processTime;
        this.traceInfo = traceInfo;
        this.actor = actor;
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
     * Получить дату постановки задачи в очередь
     *
     * @return дата постановки задачи
     */
    @Nonnull
    public ZonedDateTime getCreateDate() {
        return createDate;
    }

    /**
     * Получить данные трассировки
     *
     * @return данные трассировки
     */
    @Nullable
    public String getTraceInfo() {
        return traceInfo;
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
                reenqueueAttemptsCount == that.reenqueueAttemptsCount &&
                totalAttemptsCount == that.totalAttemptsCount &&
                Objects.equals(payload, that.payload) &&
                Objects.equals(createDate, that.createDate) &&
                Objects.equals(processTime, that.processTime) &&
                Objects.equals(traceInfo, that.traceInfo) &&
                Objects.equals(actor, that.actor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, payload, attemptsCount, reenqueueAttemptsCount, totalAttemptsCount, createDate, processTime,
                traceInfo, actor);
    }

    @Override
    public String toString() {
        return '{' +
                "id=" + id +
                ", attemptsCount=" + attemptsCount +
                ", reenqueueAttemptsCount=" + reenqueueAttemptsCount +
                ", totalAttemptsCount=" + totalAttemptsCount +
                ", createDate=" + createDate +
                ", processTime=" + processTime +
                (actor != null ? ", actor=" + actor : "") +
                '}';
    }
}
