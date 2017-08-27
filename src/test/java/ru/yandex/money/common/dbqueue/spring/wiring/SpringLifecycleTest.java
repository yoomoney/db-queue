package ru.yandex.money.common.dbqueue.spring.wiring;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.dao.QueueActorDao;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

import java.util.stream.Collectors;

import static java.lang.System.lineSeparator;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * @author Oleg Kandaurov
 * @since 12.07.2017
 */
public class SpringLifecycleTest {

    @Test
    public void should_do_actions_in_particular_order() throws Exception {
        QueueDatabaseInitializer.createTable("lifecycle_table");
        QueueDao queueDao = new QueueDao(new QueueShardId("shard1"), QueueDatabaseInitializer.getJdbcTemplate(),
                QueueDatabaseInitializer.getTransactionTemplate());
        QueueActorDao actorDao = new QueueActorDao(QueueDatabaseInitializer.getJdbcTemplate(),
                QueueDatabaseInitializer.getTransactionTemplate());
        queueDao.enqueue(SpringLifecycleConfiguration.TEST_QUEUE,
                EnqueueParams.create("first").withActor("1"));

        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();
        applicationContext.register(SpringLifecycleConfiguration.class);
        applicationContext.refresh();
        while (actorDao.isTasksExist(SpringLifecycleConfiguration.TEST_QUEUE, "1")) {
            Thread.sleep(20);
        }
        Thread.sleep(20 * 2);
        applicationContext.destroy();
        String events = SpringLifecycleConfiguration.EVENTS.stream().collect(Collectors.joining(lineSeparator()));
        System.out.println(events);
        Assert.assertThat(events,
                equalTo("queue started" + lineSeparator() +
                        "task picked on example payload=first" + lineSeparator() +
                        "running in external pool" + lineSeparator() +
                        "task started on example payload=first" + lineSeparator() +
                        "transforming to object: first" + lineSeparator() +
                        "processing task" + lineSeparator() +
                        "task executed on example payload=first" + lineSeparator() +
                        "task finished on example payload=first" + lineSeparator() +
                        "queue executed=true" + lineSeparator() +
                        "queue finished" + lineSeparator() +
                        "queue started" + lineSeparator() +
                        "queue executed=false" + lineSeparator() +
                        "queue finished" + lineSeparator() +
                        "shutting down external executor"));
    }
}
