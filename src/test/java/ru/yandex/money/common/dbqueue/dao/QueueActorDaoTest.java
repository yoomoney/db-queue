package ru.yandex.money.common.dbqueue.dao;

import org.junit.Assert;
import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Oleg Kandaurov
 * @since 11.08.2017
 */
public class QueueActorDaoTest extends BaseDaoTest {

    private final QueueActorDao queueActorDao = new QueueActorDao(jdbcTemplate);
    private final QueueDao queueDao = new QueueDao(jdbcTemplate);

    @Test
    public void should_delete_tasks_by_actor() throws Exception {
        QueueLocation location = generateUniqueLocation();
        String actor = "123";

        transactionTemplate.execute(s ->
                queueDao.enqueue(location, new EnqueueParams<String>().withActor(actor)));
        assertThat(queueActorDao.isTasksExist(location, actor), is(true));

        Boolean isDeleted = transactionTemplate.execute(s ->
                queueActorDao.deleteTasksByActor(location, actor));
        assertThat(isDeleted, is(true));
        assertThat(queueActorDao.isTasksExist(location, actor), is(false));

        Boolean isDeletedTwice = transactionTemplate.execute(s ->
                queueActorDao.deleteTasksByActor(location, actor));
        assertThat(isDeletedTwice, is(false));

    }

    @Test
    public void reenqueue_should_update_process_time() throws Exception {
        QueueLocation location = generateUniqueLocation();
        String actor = "abc123";
        Long enqueueId = executeInTransaction(() ->
                queueDao.enqueue(location, new EnqueueParams<String>().withActor(actor)));

        ZonedDateTime beforeExecution = ZonedDateTime.now();
        Duration executionDelay = Duration.ofHours(1L);
        Boolean reenqueueResult = executeInTransaction(() -> queueActorDao.reenqueue(location, actor, executionDelay));
        Assert.assertThat(reenqueueResult, equalTo(true));
        jdbcTemplate.query("select * from " + QueueDatabaseInitializer.DEFAULT_TABLE_NAME + " where id=" + enqueueId, rs -> {
            ZonedDateTime afterExecution = ZonedDateTime.now();
            Assert.assertThat(rs.next(), equalTo(true));
            ZonedDateTime processTime = ZonedDateTime.ofInstant(rs.getTimestamp("process_time").toInstant(),
                    ZoneId.systemDefault());

            Assert.assertThat(processTime.isAfter(beforeExecution.plus(executionDelay)), equalTo(true));
            Assert.assertThat(processTime.isBefore(afterExecution.plus(executionDelay)), equalTo(true));
            return new Object();
        });
    }

    @Test
    public void reenqueue_should_reset_attempts() throws Exception {
        QueueLocation location = generateUniqueLocation();
        String actor = "abc123";
        Long enqueueId = executeInTransaction(() ->
                queueDao.enqueue(location, new EnqueueParams<String>().withActor(actor)));
        executeInTransaction(() -> {
            jdbcTemplate.update("update " + QueueDatabaseInitializer.DEFAULT_TABLE_NAME + " set attempt=10 where id=" + enqueueId);
        });

        jdbcTemplate.query("select * from " + QueueDatabaseInitializer.DEFAULT_TABLE_NAME + " where id=" + enqueueId, rs -> {
            Assert.assertThat(rs.next(), equalTo(true));
            Assert.assertThat(rs.getLong("attempt"), equalTo(10L));
            return new Object();
        });

        Boolean reenqueueResult = executeInTransaction(() ->
                queueActorDao.reenqueue(location, actor, Duration.ofHours(1L)));

        Assert.assertThat(reenqueueResult, equalTo(true));
        jdbcTemplate.query("select * from " + QueueDatabaseInitializer.DEFAULT_TABLE_NAME + " where id=" + enqueueId, rs -> {
            Assert.assertThat(rs.next(), equalTo(true));
            Assert.assertThat(rs.getLong("attempt"), equalTo(0L));
            return new Object();
        });
    }

    @Test
    public void reenqueue_should_return_false_when_no_update() throws Exception {
        QueueLocation location = generateUniqueLocation();
        Boolean reenqueueResult = executeInTransaction(() ->
                queueActorDao.reenqueue(location, "...", Duration.ofHours(1L)));
        Assert.assertThat(reenqueueResult, equalTo(false));
    }
}