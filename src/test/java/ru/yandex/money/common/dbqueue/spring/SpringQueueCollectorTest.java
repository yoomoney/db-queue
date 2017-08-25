package ru.yandex.money.common.dbqueue.spring;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueProducer;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.QueueShardRouter;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.spring.impl.SpringNoopPayloadTransformer;
import ru.yandex.money.common.dbqueue.spring.impl.SpringSingleShardRouter;
import ru.yandex.money.common.dbqueue.spring.impl.SpringTransactionalProducer;

import javax.annotation.Nonnull;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @author Oleg Kandaurov
 * @since 01.08.2017
 */
public class SpringQueueCollectorTest {

    @Test
    public void should_fail_on_invalid_queue_definitions() throws Exception {
        try {
            new AnnotationConfigApplicationContext(InvalidContext.class);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("unable to collect queue beans:" + System.lineSeparator() +
                    "duplicate bean: name=testProducer2, class=SpringQueueProducer, location={queue=test_queue,table=queue_test}" + System.lineSeparator() +
                    "duplicate bean: name=testConsumer2, class=SpringQueueConsumer, location={queue=test_queue,table=queue_test}" + System.lineSeparator() +
                    "duplicate bean: name=testTransformer2, class=SpringTaskPayloadTransformer, location={queue=test_queue,table=queue_test}" + System.lineSeparator() +
                    "duplicate bean: name=testShardRouter2, class=SpringQueueShardRouter, location={queue=test_queue,table=queue_test}" + System.lineSeparator() +
                    "duplicate bean: name=queueDao2, class=QueueDao, shardId={id=1}" + System.lineSeparator() +
                    "duplicate bean: name=springTaskLifecycleListener2, class=SpringTaskLifecycleListener, location={queue=test_queue,table=queue_test}" + System.lineSeparator() +
                    "duplicate bean: name=springQueueExternalExecutor2, class=SpringQueueExternalExecutor, location={queue=test_queue,table=queue_test}"));
            return;
        }
        Assert.fail("context should not be constructed");
    }

    @Test
    public void should_collect_all_bean_definitions() throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ValidContext.class);
        SpringQueueCollector collector = context.getBean(SpringQueueCollector.class);
        assertThat(collector.getProducers().size(), equalTo(2));
        assertThat(collector.getConsumers().size(), equalTo(2));
        assertThat(collector.getTransformers().size(), equalTo(2));
        assertThat(collector.getShardRouters().size(), equalTo(2));
        assertThat(collector.getShards().size(), equalTo(2));
        assertThat(collector.getListeners().size(), equalTo(2));
        assertThat(collector.getExecutors().size(), equalTo(2));
    }

    @Configuration
    private static class ValidContext {

        private static final QueueLocation testLocation1 =
                QueueLocation.builder().withTableName("queue_test").withQueueName("test_queue1").build();
        private static final QueueLocation testLocation2 =
                QueueLocation.builder().withTableName("queue_test").withQueueName("test_queue2").build();

        public ValidContext() {
        }

        @Bean
        QueueProducer<String> testProducer1() {
            return new SpringTransactionalProducer<>(testLocation1, String.class);
        }

        @DependsOn("testProducer1")
        @Bean
        QueueProducer<String> testProducer2() {
            return new SpringTransactionalProducer<>(testLocation2, String.class);
        }

        @DependsOn("testProducer2")
        @Bean
        QueueConsumer<String> testConsumer1() {
            return new SpringQueueConsumer<String>(testLocation1, String.class) {
                @Nonnull
                @Override
                public TaskExecutionResult execute(@Nonnull Task<String> task) {
                    return TaskExecutionResult.finish();
                }
            };
        }

        @DependsOn("testConsumer1")
        @Bean
        QueueConsumer<String> testConsumer2() {
            return new SpringQueueConsumer<String>(testLocation2, String.class) {
                @Nonnull
                @Override
                public TaskExecutionResult execute(@Nonnull Task<String> task) {
                    return TaskExecutionResult.finish();
                }
            };
        }

        @DependsOn("testConsumer2")
        @Bean
        TaskPayloadTransformer<String> testTransformer1() {
            return new SpringNoopPayloadTransformer(testLocation1);
        }

        @DependsOn("testTransformer1")
        @Bean
        TaskPayloadTransformer<String> testTransformer2() {
            return new SpringNoopPayloadTransformer(testLocation2);
        }

        @DependsOn("testTransformer2")
        @Bean
        QueueShardRouter<String> testShardRouter1() {
            return new SpringSingleShardRouter<>(testLocation1,
                    String.class, mock(QueueDao.class));
        }

        @DependsOn("testShardRouter1")
        @Bean
        QueueShardRouter<String> testShardRouter2() {
            return new SpringSingleShardRouter<>(testLocation2,
                    String.class, mock(QueueDao.class));
        }

        @DependsOn("testShardRouter2")
        @Bean
        QueueDao queueDao1() {
            return new QueueDao(new QueueShardId("1"), mock(JdbcOperations.class),
                    mock(TransactionOperations.class));
        }

        @DependsOn("queueDao1")
        @Bean
        QueueDao queueDao2() {
            return new QueueDao(new QueueShardId("2"), mock(JdbcOperations.class),
                    mock(TransactionOperations.class));
        }

        @DependsOn("queueDao2")
        @Bean
        SpringTaskLifecycleListener springTaskLifecycleListener1() {
            return new NoopSpringTaskLifecycleListener(testLocation1);
        }

        @DependsOn("springTaskLifecycleListener1")
        @Bean
        SpringTaskLifecycleListener springTaskLifecycleListener2() {
            return new NoopSpringTaskLifecycleListener(testLocation2);
        }

        @DependsOn("springTaskLifecycleListener2")
        @Bean
        SpringQueueExternalExecutor springQueueExternalExecutor1() {
            SpringQueueExternalExecutor mock = mock(SpringQueueExternalExecutor.class);
            doReturn(testLocation1).when(mock).getQueueLocation();
            return mock;
        }

        @DependsOn("springQueueExternalExecutor1")
        @Bean
        SpringQueueExternalExecutor springQueueExternalExecutor2() {
            SpringQueueExternalExecutor mock = mock(SpringQueueExternalExecutor.class);
            doReturn(testLocation2).when(mock).getQueueLocation();
            return mock;
        }

        @Bean
        SpringQueueCollector springQueueCollector() {
            return new SpringQueueCollector();
        }
    }

    @Configuration
    private static class InvalidContext {
        private static final QueueLocation testLocation =
                QueueLocation.builder().withTableName("queue_test").withQueueName("test_queue").build();

        public InvalidContext() {
        }

        @Bean
        QueueProducer<String> testProducer1() {
            return new SpringTransactionalProducer<>(testLocation, String.class);
        }

        @DependsOn("testProducer1")
        @Bean
        QueueProducer<String> testProducer2() {
            return new SpringTransactionalProducer<>(testLocation, String.class);
        }

        @DependsOn("testProducer2")
        @Bean
        QueueConsumer<String> testConsumer1() {
            return new SpringQueueConsumer<String>(testLocation, String.class) {
                @Nonnull
                @Override
                public TaskExecutionResult execute(@Nonnull Task<String> task) {
                    return TaskExecutionResult.finish();
                }
            };
        }

        @DependsOn("testConsumer1")
        @Bean
        QueueConsumer<String> testConsumer2() {
            return new SpringQueueConsumer<String>(testLocation, String.class) {
                @Nonnull
                @Override
                public TaskExecutionResult execute(@Nonnull Task<String> task) {
                    return TaskExecutionResult.finish();
                }
            };
        }

        @DependsOn("testConsumer2")
        @Bean
        TaskPayloadTransformer<String> testTransformer1() {
            return new SpringNoopPayloadTransformer(testLocation);
        }

        @DependsOn("testTransformer1")
        @Bean
        TaskPayloadTransformer<String> testTransformer2() {
            return new SpringNoopPayloadTransformer(testLocation);
        }

        @DependsOn("testTransformer2")
        @Bean
        QueueShardRouter<String> testShardRouter1() {
            return new SpringSingleShardRouter<>(testLocation,
                    String.class, mock(QueueDao.class));
        }

        @DependsOn("testShardRouter1")
        @Bean
        QueueShardRouter<String> testShardRouter2() {
            return new SpringSingleShardRouter<>(testLocation,
                    String.class, mock(QueueDao.class));
        }

        @DependsOn("testShardRouter2")
        @Bean
        QueueDao queueDao1() {
            return new QueueDao(new QueueShardId("1"), mock(JdbcOperations.class),
                    mock(TransactionOperations.class));
        }

        @DependsOn("queueDao1")
        @Bean
        QueueDao queueDao2() {
            return new QueueDao(new QueueShardId("1"), mock(JdbcOperations.class),
                    mock(TransactionOperations.class));
        }

        @DependsOn("queueDao2")
        @Bean
        SpringTaskLifecycleListener springTaskLifecycleListener1() {
            return new NoopSpringTaskLifecycleListener(testLocation);
        }

        @DependsOn("springTaskLifecycleListener1")
        @Bean
        SpringTaskLifecycleListener springTaskLifecycleListener2() {
            return new NoopSpringTaskLifecycleListener(testLocation);
        }

        @DependsOn("springTaskLifecycleListener2")
        @Bean
        SpringQueueExternalExecutor springQueueExternalExecutor1() {
            SpringQueueExternalExecutor mock = mock(SpringQueueExternalExecutor.class);
            doReturn(testLocation).when(mock).getQueueLocation();
            return mock;
        }

        @DependsOn("springQueueExternalExecutor1")
        @Bean
        SpringQueueExternalExecutor springQueueExternalExecutor2() {
            SpringQueueExternalExecutor mock = mock(SpringQueueExternalExecutor.class);
            doReturn(testLocation).when(mock).getQueueLocation();
            return mock;
        }

        @Bean
        SpringQueueCollector springQueueCollector() {
            return new SpringQueueCollector();
        }
    }

    private static class NoopSpringTaskLifecycleListener extends SpringTaskLifecycleListener {

        protected NoopSpringTaskLifecycleListener(QueueLocation queueLocation) {
            super(queueLocation);
        }

        @Override
        public void picked(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord, long pickTaskTime) {

        }

        @Override
        public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord) {

        }

        @Override
        public void executed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord, @Nonnull TaskExecutionResult executionResult, long processTaskTime) {

        }

        @Override
        public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord) {

        }

        @Override
        public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord, @Nonnull Exception exc) {

        }
    }
}