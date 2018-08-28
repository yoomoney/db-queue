package ru.yandex.money.common.dbqueue.dao;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Управление задачами в очереди.
 * <p>
 * Каждый экземпляр данного класса привязан к шарду БД.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public class QueueDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    /**
     * Конструктор.
     * <p>
     * @param jdbcTemplate        spring jdbc template
     */
    public QueueDao(JdbcOperations jdbcTemplate) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
    }

    /**
     * Поставить задачу в очередь на выполнение
     *
     * @param location      местоположение очереди
     * @param enqueueParams данные вставляемой задачи
     * @return идентфикатор (sequence id) вставленной задачи
     */
    public long enqueue(@Nonnull QueueLocation location, @Nonnull EnqueueParams<String> enqueueParams) {
        requireNonNull(location);
        requireNonNull(enqueueParams);
        return jdbcTemplate.queryForObject(String.format(
                "INSERT INTO %s(queue_name, task, process_time, log_timestamp, actor) VALUES " +
                        "(:queueName, :task, now() + :executionDelay * INTERVAL '1 SECOND', " +
                        ":traceInfo, :actor) RETURNING id",
                location.getTableName()),
                new MapSqlParameterSource()
                        .addValue("queueName", location.getQueueId().asString())
                        .addValue("task", enqueueParams.getPayload())
                        .addValue("executionDelay", enqueueParams.getExecutionDelay().getSeconds())
                        .addValue("traceInfo", enqueueParams.getTraceInfo())
                        .addValue("actor", enqueueParams.getActor()),
                Long.class);
    }

    /**
     * Удалить задачу из очереди.
     *
     * @param location местоположение очеред
     * @param taskId   идентификатор (sequence id) задачи
     * @return true, если задача была удалена, false, если задача не найдена.
     */
    public boolean deleteTask(@Nonnull QueueLocation location, long taskId) {
        requireNonNull(location);
        int updatedRows = jdbcTemplate.update(String.format(
                "DELETE FROM %s WHERE queue_name = :queueName AND id = :id", location.getTableName()),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString()));
        return updatedRows != 0;
    }

    /**
     * Переставить выполнение задачи на заданный промежуток времени
     *
     * @param location       местоположение очереди
     * @param taskId         идентификатор (sequence id) задами
     * @param executionDelay промежуток времени
     * @return true, если задача была переставлена, false, если задача не найдена.
     */
    public boolean reenqueue(@Nonnull QueueLocation location, long taskId, @Nonnull Duration executionDelay) {
        requireNonNull(location);
        requireNonNull(executionDelay);
        int updatedRows = jdbcTemplate.update(String.format(
                "UPDATE %s SET  process_time = now() + :executionDelay * INTERVAL '1 SECOND',  attempt = 0 " +
                        "WHERE id = :id AND queue_name = :queueName", location.getTableName()),
                new MapSqlParameterSource()
                        .addValue("id", taskId)
                        .addValue("queueName", location.getQueueId().asString())
                        .addValue("executionDelay", executionDelay.getSeconds()));
        return updatedRows != 0;
    }


}
