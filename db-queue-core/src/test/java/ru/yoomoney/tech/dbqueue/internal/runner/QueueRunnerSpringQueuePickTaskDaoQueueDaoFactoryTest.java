package ru.yoomoney.tech.dbqueue.internal.runner;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.api.Task;
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.config.QueueShard;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.stub.StubDatabaseAccessLayer;
import ru.yoomoney.tech.dbqueue.config.TaskLifecycleListener;
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.stub.StringQueueConsumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class QueueRunnerSpringQueuePickTaskDaoQueueDaoFactoryTest {

    private static class ConsumerWithExternalExecutor extends StringQueueConsumer {

        @Nullable
        private final Executor executor;

        ConsumerWithExternalExecutor(@Nonnull QueueConfig queueConfig,
                                     @Nullable Executor executor) {
            super(queueConfig);
            this.executor = executor;
        }

        @Nonnull
        @Override
        public TaskExecutionResult execute(@Nonnull Task<String> task) {
            return TaskExecutionResult.finish();
        }

        @Override
        public Optional<Executor> getExecutor() {
            return Optional.ofNullable(executor);
        }
    }

    @Test
    public void should_return_external_executor_runner() throws Exception {
        QueueSettings settings = QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO)
                .withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR).build();
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        QueueConsumer queueConsumer = new ConsumerWithExternalExecutor(new QueueConfig(location, settings), mock(Executor.class));
        QueueRunner queueRunner = QueueRunner.Factory.create(queueConsumer,
                new QueueShard<>(new QueueShardId("s1"), new StubDatabaseAccessLayer()),
                mock(TaskLifecycleListener.class));

        assertThat(queueRunner, CoreMatchers.instanceOf(QueueRunnerInExternalExecutor.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_throw_exception_when_no_external_executor_runner() throws Exception {
        QueueSettings settings = QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO)
                .withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR).build();
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        QueueConsumer queueConsumer = new StringQueueConsumer(new QueueConfig(location, settings)) {
            @Nonnull
            @Override
            public TaskExecutionResult execute(@Nonnull Task<String> task) {
                return TaskExecutionResult.finish();
            }
        };
        QueueRunner queueRunner = QueueRunner.Factory.create(queueConsumer,
                new QueueShard<>(new QueueShardId("s1"), new StubDatabaseAccessLayer()),
                mock(TaskLifecycleListener.class));

        assertThat(queueRunner, CoreMatchers.instanceOf(QueueRunnerInExternalExecutor.class));
    }

    @Test
    public void should_return_separate_transactions_runner() throws Exception {
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        QueueSettings settings = QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO)
                .withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS).build();
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location, settings));

        QueueRunner queueRunner = QueueRunner.Factory.create(queueConsumer,
                new QueueShard<>(new QueueShardId("s1"), new StubDatabaseAccessLayer()),
                mock(TaskLifecycleListener.class));

        assertThat(queueRunner, CoreMatchers.instanceOf(QueueRunnerInSeparateTransactions.class));
    }

    @Test
    public void should_return_wrap_in_transaction_runner() throws Exception {
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        QueueSettings settings = QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO)
                .withProcessingMode(ProcessingMode.WRAP_IN_TRANSACTION).build();
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location, settings));

        QueueRunner queueRunner = QueueRunner.Factory.create(queueConsumer,
                new QueueShard<>(new QueueShardId("s1"), new StubDatabaseAccessLayer()),
                mock(TaskLifecycleListener.class));

        assertThat(queueRunner, CoreMatchers.instanceOf(QueueRunnerInTransaction.class));
    }
}