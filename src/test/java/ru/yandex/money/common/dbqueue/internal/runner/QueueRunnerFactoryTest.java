package ru.yandex.money.common.dbqueue.internal.runner;

import example.StringQueueConsumer;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.config.DatabaseDialect;
import ru.yandex.money.common.dbqueue.config.QueueShard;
import ru.yandex.money.common.dbqueue.config.QueueShardId;
import ru.yandex.money.common.dbqueue.config.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static ru.yandex.money.common.dbqueue.api.TaskExecutionResult.finish;
import static ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer.DEFAULT_SCHEMA;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class QueueRunnerFactoryTest {

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
            return finish();
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
                new QueueShard(DatabaseDialect.POSTGRESQL, DEFAULT_SCHEMA, new QueueShardId("s1"), mock(JdbcOperations.class), mock(TransactionOperations.class)),
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
                return finish();
            }
        };
        QueueRunner queueRunner = QueueRunner.Factory.create(queueConsumer,
                new QueueShard(DatabaseDialect.POSTGRESQL, DEFAULT_SCHEMA, new QueueShardId("s1"), mock(JdbcOperations.class), mock(TransactionOperations.class)),
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
                new QueueShard(DatabaseDialect.POSTGRESQL, DEFAULT_SCHEMA, new QueueShardId("s1"), mock(JdbcOperations.class), mock(TransactionOperations.class)),
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
                new QueueShard(DatabaseDialect.POSTGRESQL, DEFAULT_SCHEMA, new QueueShardId("s1"), mock(JdbcOperations.class), mock(TransactionOperations.class)),
                mock(TaskLifecycleListener.class));

        assertThat(queueRunner, CoreMatchers.instanceOf(QueueRunnerInTransaction.class));
    }
}