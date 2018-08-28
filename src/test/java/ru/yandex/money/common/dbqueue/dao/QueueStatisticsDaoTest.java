package ru.yandex.money.common.dbqueue.dao;

import org.junit.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author Oleg Kandaurov
 * @since 11.08.2017
 */
public class QueueStatisticsDaoTest extends BaseDaoTest {

    private final QueueStatisticsDao queueStatisticsDao = new QueueStatisticsDao(jdbcTemplate);


    @Test
    public void should_count_and_reset_tasks() throws Exception {
        String table = generateUniqueTable();
        QueueDatabaseInitializer.createTable(table);

        String pendingQueueName = "pending_tasks_count";

        String insertStatement = "INSERT INTO " + table +
                "(queue_name, process_time, attempt) VALUES " +
                "('" + pendingQueueName + "', %s, %d)";
        transactionTemplate.execute(s -> {
            jdbcTemplate.update(String.format(insertStatement, "now() - INTERVAL '1 MINUTE'", 0));
            jdbcTemplate.update(String.format(insertStatement, "now() - INTERVAL '1 MINUTE'", 1));
            jdbcTemplate.update(String.format(insertStatement, "now() - INTERVAL '1 MINUTE'", 2));
            jdbcTemplate.update(String.format(insertStatement, "now() + INTERVAL '1 MINUTE'", 0));
            jdbcTemplate.update(String.format(insertStatement, "now() + INTERVAL '1 MINUTE'", 1));
            jdbcTemplate.update(String.format(insertStatement, "now() + INTERVAL '1 MINUTE'", 2));
            return Void.class;
        });

        assertThat(queueStatisticsDao.getPendingTasksCount(table), equalTo(new HashMap<String, Long>() {{
            put(pendingQueueName, 3L);
        }}));
        assertThat(queueStatisticsDao.getTasksCount(table), equalTo(new HashMap<String, Long>() {{
            put(pendingQueueName, 6L);
        }}));

        Integer resettedCount = transactionTemplate.execute(s -> queueStatisticsDao.resetPendingTasks(table));
        assertThat(resettedCount, equalTo(3));

        assertThat(queueStatisticsDao.getPendingTasksCount(table), equalTo(new HashMap<String, Long>()));
        assertThat(queueStatisticsDao.getTasksCount(table), equalTo(new HashMap<String, Long>() {{
            put(pendingQueueName, 6L);
        }}));
    }

    @Test
    public void should_list_pending_tasks() throws Exception {
        String table = generateUniqueTable();
        QueueDatabaseInitializer.createTable(table);

        String pendingQueueName = "pending_tasks_list";

        transactionTemplate.execute(s -> {
            for (int i = 0; i < 15; i++) {
                new NamedParameterJdbcTemplate(jdbcTemplate).update(
                        String.format("INSERT INTO " +
                                        "%s(queue_name, task, process_time, create_time, log_timestamp, actor, attempt) " +
                                        "VALUES " +
                                        "(:queueName, :task, :processTime, :createTime, " +
                                        ":traceInfo, :actor, :attempt)",
                                table),
                        new MapSqlParameterSource()
                                .addValue("queueName", pendingQueueName)
                                .addValue("task", "task" + i)
                                .addValue("createTime", new Timestamp(100000000 * i))
                                .addValue("processTime", new Timestamp(1000000000 * i))
                                .addValue("traceInfo", "traceInfo" + i)
                                .addValue("actor", "actor" + i)
                                .addValue("attempt", 2 + i));
            }
            return Void.class;
        });

        Map<String, List<TaskRecord>> expectedPendingTasks = new LinkedHashMap<>();
        List<TaskRecord> expectedPendingTasksList = new ArrayList<>();
        expectedPendingTasks.put(pendingQueueName, expectedPendingTasksList);
        for (int id = 5; id <= 14; id++) {
            expectedPendingTasksList.add(new TaskRecord(
                    id + 1, "task" + id, id + 2,
                    ZonedDateTime.ofInstant(new Timestamp(100000000 * id).toInstant(), ZoneId.systemDefault()),
                    ZonedDateTime.ofInstant(new Timestamp(1000000000 * id).toInstant(), ZoneId.systemDefault()),
                    "traceInfo" + id, "actor" + id

            ));
        }
        Map<String, List<TaskRecord>> pendingTasks = queueStatisticsDao.listPendingTasks(table);
        assertThat(pendingTasks, equalTo(expectedPendingTasks));
    }
}