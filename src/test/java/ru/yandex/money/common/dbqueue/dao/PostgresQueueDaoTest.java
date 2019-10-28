package ru.yandex.money.common.dbqueue.dao;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

/**
 * @author Oleg Kandaurov
 * @since 12.07.2017
 */
@Ignore
public abstract class PostgresQueueDaoTest extends BaseDaoTest {

    private final PostgresQueueDao queueDao;

    public PostgresQueueDaoTest(TableSchemaType tableSchemaType) {
        super(tableSchemaType);
        queueDao = new PostgresQueueDao(jdbcTemplate, tableSchema);
    }

    @Test
    public void enqueue_should_accept_null_values() throws Exception {
        QueueLocation location = generateUniqueLocation();
        long enqueueId = queueDao.enqueue(location, new EnqueueParams<>());
        Assert.assertThat(enqueueId, not(equalTo(0)));
    }

    @Test
    public void enqueue_should_save_all_values() throws Exception {
        QueueLocation location = generateUniqueLocation();
        String payload = "{}";
        Duration executionDelay = Duration.ofHours(1L);
        ZonedDateTime beforeExecution = ZonedDateTime.now();
        long enqueueId = executeInTransaction(() -> queueDao.enqueue(location, EnqueueParams.create(payload)
                .withExecutionDelay(executionDelay)));
        jdbcTemplate.query("select * from " + tableName + " where id=" + enqueueId, rs -> {
            ZonedDateTime afterExecution = ZonedDateTime.now();
            Assert.assertThat(rs.next(), equalTo(true));
            Assert.assertThat(rs.getString(tableSchema.getPayloadField()), equalTo(payload));
            ZonedDateTime nextProcessAt = ZonedDateTime.ofInstant(rs.getTimestamp(tableSchema.getNextProcessAtField()).toInstant(),
                    ZoneId.systemDefault());
            Assert.assertThat(nextProcessAt.isAfter(beforeExecution.plus(executionDelay)), equalTo(true));
            Assert.assertThat(nextProcessAt.isBefore(afterExecution.plus(executionDelay)), equalTo(true));
            ZonedDateTime createdAt = ZonedDateTime.ofInstant(rs.getTimestamp(tableSchema.getCreatedAtField()).toInstant(),
                    ZoneId.systemDefault());
            Assert.assertThat(createdAt.isAfter(beforeExecution), equalTo(true));
            Assert.assertThat(createdAt.isBefore(afterExecution), equalTo(true));

            long reenqueueAttempt = rs.getLong(tableSchema.getReenqueueAttemptField());
            Assert.assertFalse(rs.wasNull());
            Assert.assertEquals(0L, reenqueueAttempt);

            return new Object();
        });
    }

    @Test
    public void delete_should_return_false_when_no_deletion() throws Exception {
        QueueLocation location = generateUniqueLocation();
        Boolean deleteResult = executeInTransaction(() -> queueDao.deleteTask(location, 0L));
        Assert.assertThat(deleteResult, equalTo(false));
    }

    @Test
    public void delete_should_return_true_when_deletion_occur() throws Exception {
        QueueLocation location = generateUniqueLocation();
        Long enqueueId = executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        Boolean deleteResult = executeInTransaction(() -> queueDao.deleteTask(location, enqueueId));
        Assert.assertThat(deleteResult, equalTo(true));
        jdbcTemplate.query("select * from " + tableName + " where id=" + enqueueId, rs -> {
            Assert.assertThat(rs.next(), equalTo(false));
            return new Object();
        });
    }

    @Test
    public void reenqueue_should_update_next_process_time() throws Exception {
        QueueLocation location = generateUniqueLocation();
        Long enqueueId = executeInTransaction(() ->
                queueDao.enqueue(location, new EnqueueParams<>()));

        ZonedDateTime beforeExecution = ZonedDateTime.now();
        Duration executionDelay = Duration.ofHours(1L);
        Boolean reenqueueResult = executeInTransaction(() -> queueDao.reenqueue(location, enqueueId, executionDelay));
        Assert.assertThat(reenqueueResult, equalTo(true));
        jdbcTemplate.query("select * from " + tableName + " where id=" + enqueueId, rs -> {
            ZonedDateTime afterExecution = ZonedDateTime.now();
            Assert.assertThat(rs.next(), equalTo(true));
            ZonedDateTime nextProcessAt = ZonedDateTime.ofInstant(rs.getTimestamp(tableSchema.getNextProcessAtField()).toInstant(),
                    ZoneId.systemDefault());

            Assert.assertThat(nextProcessAt.isAfter(beforeExecution.plus(executionDelay)), equalTo(true));
            Assert.assertThat(nextProcessAt.isBefore(afterExecution.plus(executionDelay)), equalTo(true));
            return new Object();
        });
    }

    @Test
    public void reenqueue_should_reset_attempts() throws Exception {
        QueueLocation location = generateUniqueLocation();
        Long enqueueId = executeInTransaction(() ->
                queueDao.enqueue(location, new EnqueueParams<>()));
        executeInTransaction(() -> {
            jdbcTemplate.update("update " + tableName + " set " + tableSchema.getAttemptField() + "=10 where id=" + enqueueId);
        });

        jdbcTemplate.query("select * from " + tableName + " where id=" + enqueueId, rs -> {
            Assert.assertThat(rs.next(), equalTo(true));
            Assert.assertThat(rs.getLong(tableSchema.getAttemptField()), equalTo(10L));
            return new Object();
        });

        Boolean reenqueueResult = executeInTransaction(() ->
                queueDao.reenqueue(location, enqueueId, Duration.ofHours(1L)));

        Assert.assertThat(reenqueueResult, equalTo(true));
        jdbcTemplate.query("select * from " + tableName + " where id=" + enqueueId, rs -> {
            Assert.assertThat(rs.next(), equalTo(true));
            Assert.assertThat(rs.getLong(tableSchema.getAttemptField()), equalTo(0L));
            return new Object();
        });
    }

    @Test
    public void reenqueue_should_increment_reenqueue_attempts() {
        QueueLocation location = generateUniqueLocation();

        Long enqueueId = executeInTransaction(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        jdbcTemplate.query("select * from " + tableName + " where id=" + enqueueId, rs -> {
            Assert.assertThat(rs.next(), equalTo(true));
            Assert.assertThat(rs.getLong(tableSchema.getReenqueueAttemptField()), equalTo(0L));
            return new Object();
        });

        Boolean reenqueueResult = executeInTransaction(() -> queueDao.reenqueue(location, enqueueId, Duration.ofHours(1L)));

        Assert.assertThat(reenqueueResult, equalTo(true));
        jdbcTemplate.query("select * from " + tableName + " where id=" + enqueueId, rs -> {
            Assert.assertThat(rs.next(), equalTo(true));
            Assert.assertThat(rs.getLong(tableSchema.getReenqueueAttemptField()), equalTo(1L));
            return new Object();
        });
    }

    @Test
    public void reenqueue_should_return_false_when_no_update() throws Exception {
        QueueLocation location = generateUniqueLocation();
        Boolean reenqueueResult = executeInTransaction(() ->
                queueDao.reenqueue(location, 0L, Duration.ofHours(1L)));
        Assert.assertThat(reenqueueResult, equalTo(false));
    }


}