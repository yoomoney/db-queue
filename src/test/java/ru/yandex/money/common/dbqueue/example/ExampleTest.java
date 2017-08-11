package ru.yandex.money.common.dbqueue.example;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.Enqueuer;

/**
 * @author Oleg Kandaurov
 * @since 12.07.2017
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExampleQueueConfiguration.class, BaseSpringQueueConfiguration.class})
public class ExampleTest {

    @Autowired
    private Enqueuer<CustomPayload> enqueuer;

    @Test
    @Ignore
    public void run_example_queue() throws Exception {
        enqueuer.enqueue(EnqueueParams.create(new CustomPayload("happy", "hello")));
        Thread.sleep(500L);
    }
}
