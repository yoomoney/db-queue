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
import ru.yandex.money.common.dbqueue.settings.QueueId;
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
                    "duplicate bean: name=testProducer2, class=SpringQueueProducer, queueId=test_queue" + System.lineSeparator() +
                    "duplicate bean: name=testConsumer2, class=SpringQueueConsumer, queueId=test_queue" + System.lineSeparator() +
                    "duplicate bean: name=testTransformer2, class=SpringTaskPayloadTransformer, queueId=test_queue" + System.lineSeparator() +
                    "duplicate bean: name=testShardRouter2, class=SpringQueueShardRouter, queueId=test_queue" + System.lineSeparator() +
                    "duplicate bean: name=queueDao2, class=QueueDao, shardId=1" + System.lineSeparator() +
                    "duplicate bean: name=springTaskLifecycleListener2, class=SpringTaskLifecycleListener, queueId=test_queue" + System.lineSeparator() +
                    "duplicate bean: name=springThreadLifecycleListener2, class=SpringThreadLifecycleListener, queueId=test_queue" + System.lineSeparator() +
                    "duplicate bean: name=springQueueExternalExecutor2, class=SpringQueueExternalExecutor, queueId=test_queue"));
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
        assertThat(collector.getTaskListeners().size(), equalTo(2));
        assertThat(collector.getThreadListeners().size(), equalTo(2));
        assertThat(collector.getExecutors().size(), equalTo(2));
    }

    @Configuration
    private static class ValidContext {

        private static final QueueId queueId1 = new QueueId("test_queue1");
        private static final QueueLocation testLocation1 =
                QueueLocation.builder().withTableName("queue_test")
                        .withQueueId(queueId1).build();

        private static final QueueId queueId2 = new QueueId("test_queue2");
        private static final QueueLocation testLocation2 =
                QueueLocation.builder().withTableName("queue_test")
                        .withQueueId(queueId2).build();

        public ValidContext() {
        }

        @Bean
        QueueProducer<String> testProducer1() {
            return new SpringTransactionalProducer<>(queueId1, String.class);
        }

        @DependsOn("testProducer1")
        @Bean
        QueueProducer<String> testProducer2() {
            return new SpringTransactionalProducer<>(queueId2, String.class);
        }

        @DependsOn("testProducer2")
        @Bean
        QueueConsumer<String> testConsumer1() {
            return new SpringQueueConsumer<String>(queueId1, String.class) {
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
            return new SpringQueueConsumer<String>(queueId2, String.class) {
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
            return new SpringNoopPayloadTransformer(queueId1);
        }

        @DependsOn("testTransformer1")
        @Bean
        TaskPayloadTransformer<String> testTransformer2() {
            return new SpringNoopPayloadTransformer(queueId2);
        }

        @DependsOn("testTransformer2")
        @Bean
        QueueShardRouter<String> testShardRouter1() {
            return new SpringSingleShardRouter<>(queueId1,
                    String.class, mock(QueueDao.class));
        }

        @DependsOn("testShardRouter1")
        @Bean
        QueueShardRouter<String> testShardRouter2() {
            return new SpringSingleShardRouter<>(queueId2,
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
            return new NoopSpringTaskLifecycleListener(queueId1);
        }

        @DependsOn("springTaskLifecycleListener1")
        @Bean
        SpringTaskLifecycleListener springTaskLifecycleListener2() {
            return new NoopSpringTaskLifecycleListener(queueId2);
        }

        @DependsOn("springTaskLifecycleListener2")
        @Bean
        SpringThreadLifecycleListener springThreadLifecycleListener1() {
            return new NoopSpringThreadLifecycleListener(queueId1);
        }

        @DependsOn("springThreadLifecycleListener1")
        @Bean
        SpringThreadLifecycleListener springThreadLifecycleListener2() {
            return new NoopSpringThreadLifecycleListener(queueId2);
        }

        @DependsOn("springThreadLifecycleListener2")
        @Bean
        SpringQueueExternalExecutor springQueueExternalExecutor1() {
            SpringQueueExternalExecutor mock = mock(SpringQueueExternalExecutor.class);
            doReturn(queueId1).when(mock).getQueueId();
            return mock;
        }

        @DependsOn("springQueueExternalExecutor1")
        @Bean
        SpringQueueExternalExecutor springQueueExternalExecutor2() {
            SpringQueueExternalExecutor mock = mock(SpringQueueExternalExecutor.class);
            doReturn(queueId2).when(mock).getQueueId();
            return mock;
        }

        @Bean
        SpringQueueCollector springQueueCollector() {
            return new SpringQueueCollector();
        }
    }

    @Configuration
    private static class InvalidContext {
        private static final QueueId queueId = new QueueId("test_queue");
        private static final QueueLocation testLocation =
                QueueLocation.builder().withTableName("queue_test")
                        .withQueueId(queueId).build();

        public InvalidContext() {
        }

        @Bean
        QueueProducer<String> testProducer1() {
            return new SpringTransactionalProducer<>(queueId, String.class);
        }

        @DependsOn("testProducer1")
        @Bean
        QueueProducer<String> testProducer2() {
            return new SpringTransactionalProducer<>(queueId, String.class);
        }

        @DependsOn("testProducer2")
        @Bean
        QueueConsumer<String> testConsumer1() {
            return new SpringQueueConsumer<String>(queueId, String.class) {
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
            return new SpringQueueConsumer<String>(queueId, String.class) {
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
            return new SpringNoopPayloadTransformer(queueId);
        }

        @DependsOn("testTransformer1")
        @Bean
        TaskPayloadTransformer<String> testTransformer2() {
            return new SpringNoopPayloadTransformer(queueId);
        }

        @DependsOn("testTransformer2")
        @Bean
        QueueShardRouter<String> testShardRouter1() {
            return new SpringSingleShardRouter<>(queueId,
                    String.class, mock(QueueDao.class));
        }

        @DependsOn("testShardRouter1")
        @Bean
        QueueShardRouter<String> testShardRouter2() {
            return new SpringSingleShardRouter<>(queueId,
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
            return new NoopSpringTaskLifecycleListener(queueId);
        }

        @DependsOn("springTaskLifecycleListener1")
        @Bean
        SpringTaskLifecycleListener springTaskLifecycleListener2() {
            return new NoopSpringTaskLifecycleListener(queueId);
        }

        @DependsOn("springTaskLifecycleListener2")
        @Bean
        SpringThreadLifecycleListener springThreadLifecycleListener1() {
            return new NoopSpringThreadLifecycleListener(queueId);
        }

        @DependsOn("springThreadLifecycleListener1")
        @Bean
        SpringThreadLifecycleListener springThreadLifecycleListener2() {
            return new NoopSpringThreadLifecycleListener(queueId);
        }

        @DependsOn("springThreadLifecycleListener2")
        @Bean
        SpringQueueExternalExecutor springQueueExternalExecutor1() {
            SpringQueueExternalExecutor mock = mock(SpringQueueExternalExecutor.class);
            doReturn(queueId).when(mock).getQueueId();
            return mock;
        }

        @DependsOn("springQueueExternalExecutor1")
        @Bean
        SpringQueueExternalExecutor springQueueExternalExecutor2() {
            SpringQueueExternalExecutor mock = mock(SpringQueueExternalExecutor.class);
            doReturn(queueId).when(mock).getQueueId();
            return mock;
        }

        @Bean
        SpringQueueCollector springQueueCollector() {
            return new SpringQueueCollector();
        }
    }

    private static class NoopSpringTaskLifecycleListener extends SpringTaskLifecycleListener {

        protected NoopSpringTaskLifecycleListener(QueueId queueId) {
            super(queueId);
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

    private static class NoopSpringThreadLifecycleListener extends SpringThreadLifecycleListener {

        protected NoopSpringThreadLifecycleListener(QueueId queueId) {
            super(queueId);
        }

        @Override
        public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {

        }

        @Override
        public void executed(QueueShardId shardId, QueueLocation location, boolean taskProcessed, long threadBusyTime) {

        }

        @Override
        public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {

        }

        @Override
        public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull Throwable exc) {

        }
    }
}