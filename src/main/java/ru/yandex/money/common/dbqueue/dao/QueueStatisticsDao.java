package ru.yandex.money.common.dbqueue.dao;

import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yandex.money.common.dbqueue.api.TaskRecord;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Получение статистических данных по выполнениею очередей и
 * функции управления для использования в административном интерфейсе
 *
 * @author Oleg Kandaurov
 * @since 10.07.2017
 */
public class QueueStatisticsDao {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    /**
     * Конструктор
     *
     * @param jdbcTemplate        spring jdbc template
     */
    public QueueStatisticsDao(JdbcOperations jdbcTemplate) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
    }

    /**
     * Получить количество задач, у которых была минимум одна неуспешная обработка.
     *
     * @param tableName таблица с задачами
     * @return Map: key = имя очереди, value: количество зависших задач в очереди
     */
    public Map<String, Long> getPendingTasksCount(String tableName) {
        Map<String, Long> pendingTasks = new LinkedHashMap<>();
        jdbcTemplate.query(String.format("SELECT queue_name, COUNT(1) FROM %s " +
                "WHERE (attempt > 1) OR (attempt = 1 AND process_time < now()) " +
                "GROUP BY queue_name ORDER BY queue_name", tableName), rs -> {
            pendingTasks.put(rs.getString(1), rs.getLong(2));
        });
        return pendingTasks;
    }

    /**
     * Получить количество всех задач в очереди
     *
     * @param tableName таблица с задачами
     * @return Map: key = имя очереди, value: количество задач в очереди
     */
    public Map<String, Long> getTasksCount(String tableName) {
        Map<String, Long> tasks = new LinkedHashMap<>();
        jdbcTemplate.query(String.format("SELECT queue_name, COUNT(1) FROM %s " +
                "GROUP BY queue_name ORDER BY queue_name", tableName), rs -> {
            tasks.put(rs.getString(1), rs.getLong(2));
        });
        return tasks;
    }

    /**
     * Получить данные задач, у которых была минимум одна неуспешная попытка выполнения.
     * <p>
     * Выборка ограничивается последними десятью задачами.
     *
     * @param tableName таблица с задачами
     * @return Map: key = имя очереди, value: данные зависших задач
     */
    public Map<String, List<TaskRecord>> listPendingTasks(String tableName) {
        Map<String, List<TaskRecord>> result = new HashMap<>();
        jdbcTemplate.query(String.format(
                "SELECT t.id, t.queue_name, substring(t.task for 2000) as payload, t.process_time, " +
                        "t.attempt, t.reenqueue_attempt, t.total_attempt, t.create_time, t.log_timestamp as trace_info, t.actor " +
                        "FROM %s t JOIN (SELECT id, row_number() OVER (PARTITION BY queue_name ORDER BY id desc) rn " +
                        "FROM %s WHERE (attempt > 1) OR (attempt = 1 AND process_time < now())) task_ids " +
                        "on task_ids.id=t.id WHERE task_ids.rn<=10", tableName, tableName), rs -> {
            String queueName = rs.getString("queue_name");
            if (!result.containsKey(queueName)) {
                result.put(queueName, new ArrayList<>(10));
            }

            result.get(queueName).add(new TaskRecord(
                    rs.getLong("id"),
                    rs.getString("payload"),
                    rs.getLong("attempt"),
                    rs.getLong("reenqueue_attempt"),
                    rs.getLong("total_attempt"),
                    ZonedDateTime.ofInstant(rs.getTimestamp("create_time").toInstant(),
                            ZoneId.systemDefault()),
                    ZonedDateTime.ofInstant(rs.getTimestamp("process_time").toInstant(),
                            ZoneId.systemDefault()),
                    rs.getString("trace_info"),
                    rs.getString("actor")));
        });
        return result;
    }

    /**
     * Выполнить задачи, у которых была минимум одна неуспешная попытка выполнения.
     * <p>
     * Метод обнуляет попытки выполнения задач в заданной таблице,
     * что приводит к их повторному взятию на обработку.
     *
     * @param tableName таблица с задачами
     * @return количество сброшенных задач
     */
    public int resetPendingTasks(String tableName) {
        return jdbcTemplate.update(String.format("UPDATE %s SET process_time=now(), attempt = 0 " +
                        "WHERE (attempt > 1) OR (attempt = 1 AND process_time < now())", tableName),
                Collections.emptyMap());
    }

}
