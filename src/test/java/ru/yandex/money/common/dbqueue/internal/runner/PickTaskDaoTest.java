package ru.yandex.money.common.dbqueue.internal.runner;

import org.junit.Assert;
import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.dao.BaseDaoTest;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

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
 * @since 15.07.2017
 */
public class PickTaskDaoTest extends BaseDaoTest {

    private final QueueDao queueDao = new QueueDao(jdbcTemplate);
    private final PickTaskDao pickTaskDao = new PickTaskDao(new QueueShardId("s1"), jdbcTemplate, transactionTemplate);

    /**
     * Из-за особенностей windows какая-то фигня со временем БД
     */
    private final static Duration WINDOWS_OS_DELAY = Duration.ofSeconds(2);

    @Test
    public void should_not_pick_task_too_early() throws Exception {
        QueueLocation location = generateUniqueLocation();
        executeInTransaction(() ->
                queueDao.enqueue(location, new EnqueueParams<String>().withExecutionDelay(Duration.ofHours(1))));
        TaskRecord taskRecord = pickTaskDao.pickTask(location, new RetryTaskStrategy.ArithmeticBackoff(
                QueueSettings.builder().withNoTaskTimeout(Duration.ZERO)
                        .withBetweenTaskTimeout(Duration.ZERO).build()
        ));
        Assert.assertThat(taskRecord, is(nullValue()));
    }

    @Test
    public void pick_task_should_return_all_fields() throws Exception {
        QueueLocation location = generateUniqueLocation();
        String payload = "{}";
        String traceInfo = "#11";
        String actor = "id-123";
        ZonedDateTime beforeEnqueue = ZonedDateTime.now();
        long enqueueId = executeInTransaction(() -> queueDao.enqueue(location,
                EnqueueParams.create(payload).withTraceInfo(traceInfo).withActor(actor)));

        TaskRecord taskRecord = null;
        while (taskRecord == null) {
            taskRecord = executeInTransaction(
                    () -> pickTaskDao.pickTask(location, new RetryTaskStrategy.ArithmeticBackoff(
                            QueueSettings.builder().withNoTaskTimeout(Duration.ZERO)
                                    .withBetweenTaskTimeout(Duration.ZERO).build()
                    )));
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        ZonedDateTime afterEnqueue = ZonedDateTime.now();
        Assert.assertThat(taskRecord, is(not(nullValue())));
        Objects.requireNonNull(taskRecord);
        Assert.assertThat(taskRecord.getActor(), equalTo(actor));
        Assert.assertThat(taskRecord.getAttemptsCount(), equalTo(1L));
        Assert.assertThat(taskRecord.getTraceInfo(), equalTo(traceInfo));
        Assert.assertThat(taskRecord.getId(), equalTo(enqueueId));
        Assert.assertThat(taskRecord.getPayload(), equalTo(payload));
        Assert.assertThat(taskRecord.getProcessTime(), is(not(nullValue())));
        Assert.assertThat(taskRecord.getCreateDate().isAfter(beforeEnqueue), equalTo(true));
        Assert.assertThat(taskRecord.getCreateDate().isBefore(afterEnqueue), equalTo(true));
    }

    @Test
    public void pick_task_should_delay_with_linear_strategy() {
        QueueLocation location = generateUniqueLocation();
        Duration expectedDelay = Duration.ofMinutes(3L);
        ZonedDateTime beforePickingTask;
        ZonedDateTime afterPickingTask;
        TaskRecord taskRecord;
        RetryTaskStrategy retryTaskStrategy = new RetryTaskStrategy.LinearBackoff(
                QueueSettings.builder().withNoTaskTimeout(Duration.ZERO)
                        .withBetweenTaskTimeout(Duration.ZERO)
                        .withRetryInterval(Duration.ofMinutes(3))
                        .build());

        Long enqueueId = executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        for (int attempt = 1; attempt < 10; attempt++) {
            beforePickingTask = ZonedDateTime.now();
            taskRecord = resetProcessTimeAndPick(location, retryTaskStrategy, enqueueId);
            afterPickingTask = ZonedDateTime.now();
            Assert.assertThat(taskRecord.getAttemptsCount(), equalTo((long) attempt));
            Assert.assertThat(taskRecord.getProcessTime().isAfter(beforePickingTask.plus(expectedDelay.minus(WINDOWS_OS_DELAY))), equalTo(true));
            Assert.assertThat(taskRecord.getProcessTime().isBefore(afterPickingTask.plus(expectedDelay).plus(WINDOWS_OS_DELAY)), equalTo(true));
        }
    }

    @Test
    public void pick_task_should_delay_with_arithmetic_strategy() {
        QueueLocation location = generateUniqueLocation();
        Duration expectedDelay;
        ZonedDateTime beforePickingTask;
        ZonedDateTime afterPickingTask;
        TaskRecord taskRecord;

        RetryTaskStrategy retryTaskStrategy = new RetryTaskStrategy.ArithmeticBackoff(
                QueueSettings.builder().withNoTaskTimeout(Duration.ZERO)
                        .withBetweenTaskTimeout(Duration.ZERO)
                        .build()
        );


        Long enqueueId = executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        for (int attempt = 1; attempt < 10; attempt++) {
            beforePickingTask = ZonedDateTime.now();
            taskRecord = resetProcessTimeAndPick(location, retryTaskStrategy, enqueueId);
            afterPickingTask = ZonedDateTime.now();
            expectedDelay = Duration.ofMinutes((long) (1 + (attempt - 1) * 2));
            Assert.assertThat(taskRecord.getAttemptsCount(), equalTo((long) attempt));
            Assert.assertThat(taskRecord.getProcessTime().isAfter(beforePickingTask.plus(expectedDelay.minus(WINDOWS_OS_DELAY))), equalTo(true));
            Assert.assertThat(taskRecord.getProcessTime().isBefore(afterPickingTask.plus(expectedDelay.plus(WINDOWS_OS_DELAY))), equalTo(true));
        }
    }

    @Test
    public void pick_task_should_delay_with_geometric_strategy() {
        QueueLocation location = generateUniqueLocation();
        Duration expectedDelay;
        ZonedDateTime beforePickingTask;
        ZonedDateTime afterPickingTask;
        TaskRecord taskRecord;

        RetryTaskStrategy retryTaskStrategy = new RetryTaskStrategy.GeometricBackoff(
                QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO)
                        .withNoTaskTimeout(Duration.ZERO).build()
        );

        Long enqueueId = executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        for (int attempt = 1; attempt < 10; attempt++) {
            beforePickingTask = ZonedDateTime.now();
            taskRecord = resetProcessTimeAndPick(location, retryTaskStrategy, enqueueId);
            afterPickingTask = ZonedDateTime.now();
            expectedDelay = Duration.ofMinutes(BigInteger.valueOf(2L).pow(attempt - 1).longValue());
            Assert.assertThat(taskRecord.getAttemptsCount(), equalTo((long) attempt));
            Assert.assertThat(taskRecord.getProcessTime().isAfter(beforePickingTask.plus(expectedDelay.minus(WINDOWS_OS_DELAY))), equalTo(true));
            Assert.assertThat(taskRecord.getProcessTime().isBefore(afterPickingTask.plus(expectedDelay.plus(WINDOWS_OS_DELAY))), equalTo(true));
        }
    }

    private TaskRecord resetProcessTimeAndPick(QueueLocation location, RetryTaskStrategy retryTaskStrategy, Long enqueueId) {
        executeInTransaction(() -> {
            jdbcTemplate.update("update " + QueueDatabaseInitializer.DEFAULT_TABLE_NAME +
                    " set process_time=? where id=" + enqueueId, new Timestamp(new Date().getTime()));
        });

        TaskRecord taskRecord = null;
        while (taskRecord == null) {
            taskRecord = executeInTransaction(
                    () -> pickTaskDao.pickTask(location, retryTaskStrategy));
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return taskRecord;
    }
}
