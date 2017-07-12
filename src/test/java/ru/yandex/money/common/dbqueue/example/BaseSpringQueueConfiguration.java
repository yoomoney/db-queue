package ru.yandex.money.common.dbqueue.example;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.money.common.dbqueue.api.QueueThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.init.QueueExecutionPool;
import ru.yandex.money.common.dbqueue.init.QueueRegistry;
import ru.yandex.money.common.dbqueue.spring.SpringQueueCollector;
import ru.yandex.money.common.dbqueue.spring.SpringQueueConfigContainer;
import ru.yandex.money.common.dbqueue.spring.SpringQueueInitializer;
import ru.yandex.money.common.dbqueue.spring.SpringQueueStarter;

/**
 * @author Oleg Kandaurov
 * @since 20.07.2017
 */
@Configuration
public class BaseSpringQueueConfiguration {

    @Bean
    QueueThreadLifecycleListener queueThreadLifecycleListener() {
        return new CustomQueueThreadLifecycleListener();
    }

    @Bean
    TaskLifecycleListener defaultTaskLifecycleListener() {
        return new CustomTaskLifecycleListener();
    }

    @Bean
    QueueRegistry queueRegistry() {
        return new QueueRegistry();
    }

    @Bean
    SpringQueueCollector springQueueCollector() {
        return new SpringQueueCollector();
    }

    @Bean
    SpringQueueInitializer springQueueInitializer(SpringQueueConfigContainer springQueueConfigContainer) {
        return new SpringQueueInitializer(springQueueConfigContainer, springQueueCollector(), queueRegistry());
    }

    @Bean
    QueueExecutionPool queueExecutionPool() {
        return new QueueExecutionPool(queueRegistry(), defaultTaskLifecycleListener(), queueThreadLifecycleListener());
    }

    @Bean
    SpringQueueStarter springQueueStarter() {
        return new SpringQueueStarter(queueExecutionPool());
    }


}
