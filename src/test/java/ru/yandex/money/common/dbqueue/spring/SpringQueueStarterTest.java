package ru.yandex.money.common.dbqueue.spring;

import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.money.common.dbqueue.init.QueueExecutionPool;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Oleg Kandaurov
 * @since 02.08.2017
 */
public class SpringQueueStarterTest {

    private static QueueExecutionPool poolForStart = mock(QueueExecutionPool.class);
    private static QueueExecutionPool poolForShutdown = mock(QueueExecutionPool.class);

    @Test
    public void should_start_queues() throws Exception {
        new AnnotationConfigApplicationContext(StartContext.class);
        verify(poolForStart).init();
        verify(poolForStart).start();
    }

    @Test
    public void should_stop_queues() throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ShutdownContext.class);
        context.destroy();
        verify(poolForShutdown).start();
        verify(poolForShutdown).shutdown();
    }

    @Configuration
    public static class StartContext {

        public StartContext() {
        }

        @Bean
        SpringQueueStarter springQueueStarter() {
            return new SpringQueueStarter(poolForStart);
        }
    }

    @Configuration
    public static class ShutdownContext {

        public ShutdownContext() {
        }

        @Bean
        SpringQueueStarter springQueueStarter() {
            return new SpringQueueStarter(poolForShutdown);
        }
    }
}