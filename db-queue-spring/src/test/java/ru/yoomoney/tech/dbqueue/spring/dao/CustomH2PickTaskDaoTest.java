package ru.yoomoney.tech.dbqueue.spring.dao;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.dao.PickTaskSettings;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.TaskRetryType;
import ru.yoomoney.tech.dbqueue.spring.dao.utils.H2DatabaseInitializer;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.*;

public class CustomH2PickTaskDaoTest extends QueuePickTaskDaoTest {

    @BeforeClass
    public static void beforeClass() {
        H2DatabaseInitializer.initialize();
    }

    public CustomH2PickTaskDaoTest() {
        super(
                new H2QueueDao(
                        H2DatabaseInitializer.getJdbcTemplate(),
                        H2DatabaseInitializer.CUSTOM_SCHEMA),
                pickTaskSettings ->
                        new H2QueuePickTaskDao(
                                H2DatabaseInitializer.getJdbcTemplate(),
                                H2DatabaseInitializer.CUSTOM_SCHEMA,
                                pickTaskSettings,
                                H2DatabaseInitializer.getTransactionTemplate()),
                H2DatabaseInitializer.CUSTOM_TABLE_NAME,
                H2DatabaseInitializer.CUSTOM_SCHEMA,
                H2DatabaseInitializer.getJdbcTemplate(),
                H2DatabaseInitializer.getTransactionTemplate());
    }

    @Override
    protected String currentTimeSql() {
        return "now()";
    }


    @Test
    public void pick_task_concurrently() throws Exception {
        final QueueLocation location = generateUniqueLocation();
        final String payload = "{}";
        final ZonedDateTime beforeEnqueue = ZonedDateTime.now();

        final int taskCount = 20;
        final Set<Long> taskIds = IntStream
                .range(0, taskCount)
                .mapToObj(ignored -> executeInTransaction(() -> queueDao.enqueue(location, EnqueueParams.create(payload))))
                .collect(Collectors.toSet());

        final QueuePickTaskDao pickTaskDao = pickTaskDaoFactory.apply(
                new PickTaskSettings(
                        TaskRetryType.ARITHMETIC_BACKOFF,
                        Duration.ofMinutes(1)));
        final List<TaskRecord> records = IntStream
                .range(0, taskCount)
                .parallel()
                .mapToObj(e -> {
                    TaskRecord taskRecord;
                    do {
                        taskRecord = executeInTransaction(() -> pickTaskDao.pickTask(location));
                        try {
                            Thread.sleep(20);
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    } while (taskRecord == null);
                    return taskRecord;
                })
                .collect(Collectors.toList());

        records.forEach(taskRecord -> {
            ZonedDateTime afterEnqueue = ZonedDateTime.now();
            Assert.assertThat(taskRecord, is(not(nullValue())));
            Objects.requireNonNull(taskRecord);
            Assert.assertThat(taskRecord.getAttemptsCount(), equalTo(1L));
            Assert.assertThat(taskRecord.getPayload(), equalTo(payload));
            Assert.assertThat(taskRecord.getNextProcessAt(), is(not(nullValue())));
            Assert.assertThat(taskRecord.getCreatedAt().isAfter(beforeEnqueue), equalTo(true));
            Assert.assertThat(taskRecord.getCreatedAt().isBefore(afterEnqueue), equalTo(true));

            Assert.assertTrue(taskIds.remove(taskRecord.getId()));
        });
        Assert.assertTrue(taskIds.isEmpty());
    }
}
