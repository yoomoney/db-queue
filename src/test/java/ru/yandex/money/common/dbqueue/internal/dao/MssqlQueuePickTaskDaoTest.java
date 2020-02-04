package ru.yandex.money.common.dbqueue.internal.dao;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.dao.BaseDaoTest;
import ru.yandex.money.common.dbqueue.dao.MssqlQueueDao;
import ru.yandex.money.common.dbqueue.internal.pick.PickTaskSettings;
import ru.yandex.money.common.dbqueue.internal.pick.MssqlQueuePickTaskDao;
import ru.yandex.money.common.dbqueue.internal.pick.QueuePickTaskDao;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.TaskRetryType;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;

/**
 * @author Oleg Kandaurov
 * @author Behrooz Shabani
 * @since 25.01.2020
 */
@Ignore
public abstract class MssqlQueuePickTaskDaoTest extends BaseDaoTest {

    private final MssqlQueueDao queueDao = new MssqlQueueDao(jdbcTemplate, tableSchema);

    /**
     * Из-за особенностей windows какая-то фигня со временем БД
     */
    private final static Duration WINDOWS_OS_DELAY = Duration.ofSeconds(2);

    public MssqlQueuePickTaskDaoTest(TableSchemaType tableSchemaType) {
        super(tableSchemaType);
    }

    @Test
    public void should_not_pick_task_too_early() throws Exception {
        QueueLocation location = generateUniqueLocation();
        executeInTransaction(() ->
                queueDao.enqueue(location, new EnqueueParams<String>().withExecutionDelay(Duration.ofHours(1))));
        MssqlQueuePickTaskDao pickTaskDao = new MssqlQueuePickTaskDao(jdbcTemplate, tableSchema,
                new PickTaskSettings(TaskRetryType.ARITHMETIC_BACKOFF, Duration.ofMinutes(1)));
        TaskRecord taskRecord = pickTaskDao.pickTask(location);
        Assert.assertThat(taskRecord, is(nullValue()));
    }

    @Test
    public void pick_task_should_return_all_fields() throws Exception {
        QueueLocation location = generateUniqueLocation();
        String payload = "{}";
        ZonedDateTime beforeEnqueue = ZonedDateTime.now();
        long enqueueId = executeInTransaction(() -> queueDao.enqueue(location,
                EnqueueParams.create(payload)));

        TaskRecord taskRecord = null;
        MssqlQueuePickTaskDao pickTaskDao = new MssqlQueuePickTaskDao(jdbcTemplate,
                tableSchema, new PickTaskSettings(TaskRetryType.ARITHMETIC_BACKOFF, Duration.ofMinutes(1)));
        while (taskRecord == null) {
            taskRecord = executeInTransaction(() -> pickTaskDao.pickTask(location));
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        ZonedDateTime afterEnqueue = ZonedDateTime.now();
        Assert.assertThat(taskRecord, is(not(nullValue())));
        Objects.requireNonNull(taskRecord);
        Assert.assertThat(taskRecord.getAttemptsCount(), equalTo(1L));
        Assert.assertThat(taskRecord.getId(), equalTo(enqueueId));
        Assert.assertThat(taskRecord.getPayload(), equalTo(payload));
        Assert.assertThat(taskRecord.getNextProcessAt(), is(not(nullValue())));
        Assert.assertThat(taskRecord.getCreatedAt().isAfter(beforeEnqueue), equalTo(true));
        Assert.assertThat(taskRecord.getCreatedAt().isBefore(afterEnqueue), equalTo(true));
    }

    @Test
    public void pick_task_should_delay_with_linear_strategy() {
        QueueLocation location = generateUniqueLocation();
        Duration expectedDelay = Duration.ofMinutes(3L);
        ZonedDateTime beforePickingTask;
        ZonedDateTime afterPickingTask;
        TaskRecord taskRecord;
        PickTaskSettings pickTaskSettings = new PickTaskSettings(TaskRetryType.LINEAR_BACKOFF, Duration.ofMinutes(3));
        MssqlQueuePickTaskDao pickTaskDao = new MssqlQueuePickTaskDao(jdbcTemplate,
                tableSchema, pickTaskSettings);

        Long enqueueId = executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        for (int attempt = 1; attempt < 10; attempt++) {
            beforePickingTask = ZonedDateTime.now();
            taskRecord = resetProcessTimeAndPick(location, pickTaskDao, enqueueId);
            afterPickingTask = ZonedDateTime.now();
            Assert.assertThat(taskRecord.getAttemptsCount(), equalTo((long) attempt));
            Assert.assertThat(taskRecord.getNextProcessAt().isAfter(beforePickingTask.plus(expectedDelay.minus(WINDOWS_OS_DELAY))), equalTo(true));
            Assert.assertThat(taskRecord.getNextProcessAt().isBefore(afterPickingTask.plus(expectedDelay).plus(WINDOWS_OS_DELAY)), equalTo(true));
        }
    }

    @Test
    public void pick_task_should_delay_with_arithmetic_strategy() {
        QueueLocation location = generateUniqueLocation();
        Duration expectedDelay;
        ZonedDateTime beforePickingTask;
        ZonedDateTime afterPickingTask;
        TaskRecord taskRecord;

        PickTaskSettings pickTaskSettings = new PickTaskSettings(TaskRetryType.ARITHMETIC_BACKOFF, Duration.ofMinutes(1));
        MssqlQueuePickTaskDao pickTaskDao = new MssqlQueuePickTaskDao(jdbcTemplate,
                tableSchema, pickTaskSettings);

        Long enqueueId = executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        for (int attempt = 1; attempt < 10; attempt++) {
            beforePickingTask = ZonedDateTime.now();
            taskRecord = resetProcessTimeAndPick(location, pickTaskDao, enqueueId);
            afterPickingTask = ZonedDateTime.now();
            expectedDelay = Duration.ofMinutes(1 + (attempt - 1) * 2);
            Assert.assertThat(taskRecord.getAttemptsCount(), equalTo((long) attempt));
            Assert.assertThat(taskRecord.getNextProcessAt().isAfter(beforePickingTask.plus(expectedDelay.minus(WINDOWS_OS_DELAY))), equalTo(true));
            Assert.assertThat(taskRecord.getNextProcessAt().isBefore(afterPickingTask.plus(expectedDelay.plus(WINDOWS_OS_DELAY))), equalTo(true));
        }
    }

    @Test
    public void pick_task_should_delay_with_geometric_strategy() {
        QueueLocation location = generateUniqueLocation();
        Duration expectedDelay;
        ZonedDateTime beforePickingTask;
        ZonedDateTime afterPickingTask;
        TaskRecord taskRecord;

        PickTaskSettings pickTaskSettings = new PickTaskSettings(TaskRetryType.GEOMETRIC_BACKOFF, Duration.ofMinutes(1));
        MssqlQueuePickTaskDao pickTaskDao = new MssqlQueuePickTaskDao(jdbcTemplate,
                tableSchema, pickTaskSettings);

        Long enqueueId = executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        for (int attempt = 1; attempt < 10; attempt++) {
            beforePickingTask = ZonedDateTime.now();
            taskRecord = resetProcessTimeAndPick(location, pickTaskDao, enqueueId);
            afterPickingTask = ZonedDateTime.now();
            expectedDelay = Duration.ofMinutes(BigInteger.valueOf(2L).pow(attempt - 1).longValue());
            Assert.assertThat(taskRecord.getAttemptsCount(), equalTo((long) attempt));
            Assert.assertThat(taskRecord.getNextProcessAt().isAfter(beforePickingTask.plus(expectedDelay.minus(WINDOWS_OS_DELAY))), equalTo(true));
            Assert.assertThat(taskRecord.getNextProcessAt().isBefore(afterPickingTask.plus(expectedDelay.plus(WINDOWS_OS_DELAY))), equalTo(true));
        }
    }

    private TaskRecord resetProcessTimeAndPick(QueueLocation location, QueuePickTaskDao pickTaskDao, Long enqueueId) {
        executeInTransaction(() -> {
            jdbcTemplate.update("update " + tableName +
                    " set " + tableSchema.getNextProcessAtField() + "= SYSDATETIMEOFFSET() where id=" + enqueueId);
        });

        TaskRecord taskRecord = null;
        while (taskRecord == null) {
            taskRecord = executeInTransaction(
                    () -> pickTaskDao.pickTask(location));
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return taskRecord;
    }
}
