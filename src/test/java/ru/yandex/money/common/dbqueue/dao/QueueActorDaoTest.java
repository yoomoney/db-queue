package ru.yandex.money.common.dbqueue.dao;

import org.junit.Test;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Oleg Kandaurov
 * @since 11.08.2017
 */
public class QueueActorDaoTest extends BaseDaoTest {

    private final QueueActorDao queueActorDao = new QueueActorDao(jdbcTemplate, transactionTemplate);
    private final QueueDao queueDao = new QueueDao(new QueueShardId("s1"), jdbcTemplate, transactionTemplate);

    @Test
    public void should_delete_tasks_by_actor() throws Exception {
        QueueLocation location = generateUniqueLocation();
        String actor = "123";

        queueDao.getTransactionTemplate().execute(s ->
                queueDao.enqueue(location, new EnqueueParams<String>().withActor(actor)));
        assertThat(queueActorDao.isTasksExist(location, actor), is(true));

        Boolean isDeleted = queueActorDao.getTransactionTemplate().execute(s ->
                queueActorDao.deleteTasksByActor(location, actor));
        assertThat(isDeleted, is(true));
        assertThat(queueActorDao.isTasksExist(location, actor), is(false));

        Boolean isDeletedTwice = queueActorDao.getTransactionTemplate().execute(s ->
                queueActorDao.deleteTasksByActor(location, actor));
        assertThat(isDeletedTwice, is(false));

    }
}