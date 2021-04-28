package ru.yoomoney.tech.dbqueue.spring.dao;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.dao.PickTaskSettings;
import ru.yoomoney.tech.dbqueue.dao.QueueDao;
import ru.yoomoney.tech.dbqueue.dao.QueuePickTaskDao;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.TaskRetryType;

import java.math.BigInteger;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;

/**
 * @author Oleg Kandaurov
 * @since 15.07.2017
 */
@Ignore
public abstract class QueuePickTaskDaoTest {

    protected final JdbcTemplate jdbcTemplate;
    protected final TransactionTemplate transactionTemplate;

    protected final String tableName;
    protected final QueueTableSchema tableSchema;

    private final QueueDao queueDao;
    private final Function<PickTaskSettings, QueuePickTaskDao> pickTaskDaoFactory;

    /**
     * Из-за особенностей windows какая-то фигня со временем БД
     */
    private final static Duration WINDOWS_OS_DELAY = Duration.ofSeconds(2);

    public QueuePickTaskDaoTest(QueueDao queueDao, Function<PickTaskSettings, QueuePickTaskDao> pickTaskDaoFactory,
                                String tableName, QueueTableSchema tableSchema,
                                JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.transactionTemplate = transactionTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.queueDao = queueDao;
        this.pickTaskDaoFactory = pickTaskDaoFactory;
    }

    @Test
    public void should_not_pick_task_too_early() throws Exception {
        QueueLocation location = generateUniqueLocation();
        executeInTransaction(() ->
                queueDao.enqueue(location, new EnqueueParams<String>().withExecutionDelay(Duration.ofHours(1))));
        QueuePickTaskDao pickTaskDao = pickTaskDaoFactory.apply(new PickTaskSettings(TaskRetryType.ARITHMETIC_BACKOFF, Duration.ofMinutes(1)));
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
        QueuePickTaskDao pickTaskDao = pickTaskDaoFactory.apply(new PickTaskSettings(TaskRetryType.ARITHMETIC_BACKOFF, Duration.ofMinutes(1)));
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
        QueuePickTaskDao pickTaskDao = pickTaskDaoFactory.apply(pickTaskSettings);

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
        QueuePickTaskDao pickTaskDao = pickTaskDaoFactory.apply(pickTaskSettings);

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
        QueuePickTaskDao pickTaskDao = pickTaskDaoFactory.apply(pickTaskSettings);

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
                    " set " + tableSchema.getNextProcessAtField() + "= " + currentTimeSql() + " where " + tableSchema.getIdField() + "=" + enqueueId);
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

    protected abstract String currentTimeSql();

    protected QueueLocation generateUniqueLocation() {
        return QueueLocation.builder().withTableName(tableName)
                .withQueueId(new QueueId("test-queue-" + UUID.randomUUID())).build();
    }

    protected void executeInTransaction(Runnable runnable) {
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                runnable.run();
            }
        });
    }

    protected <T> T executeInTransaction(Supplier<T> supplier) {
        return transactionTemplate.execute(status -> supplier.get());
    }

}
