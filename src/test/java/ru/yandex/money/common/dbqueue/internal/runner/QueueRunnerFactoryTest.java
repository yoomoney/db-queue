package ru.yandex.money.common.dbqueue.internal.runner;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.support.TransactionOperations;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;

import java.time.Duration;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class QueueRunnerFactoryTest {

    @Test
    public void should_return_external_executor_runner() throws Exception {
        QueueConsumer queueConsumer = mock(QueueConsumer.class);
        QueueSettings settings = QueueSettings.builder().withBetweenTaskTimeout(Duration.ZERO).withNoTaskTimeout(Duration.ZERO)
                .withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR).build();
        QueueLocation location = QueueLocation.builder().withTableName("testTable")
                .withQueueId(new QueueId("testQueue")).build();
        when(queueConsumer.getQueueConfig()).thenReturn(new QueueConfig(location, settings));

        QueueRunner queueRunner = QueueRunner.Factory.createQueueRunner(queueConsumer,
                new QueueDao(new QueueShardId("s1"), mock(JdbcOperations.class), mock(TransactionOperations.class)),
                mock(TaskLifecycleListener.class),
                mock(Executor.class));

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

        QueueRunner queueRunner = QueueRunner.Factory.createQueueRunner(queueConsumer,
                new QueueDao(new QueueShardId("s1"), mock(JdbcOperations.class), mock(TransactionOperations.class)),
                mock(TaskLifecycleListener.class),
                null);

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

        QueueRunner queueRunner = QueueRunner.Factory.createQueueRunner(queueConsumer,
                new QueueDao(new QueueShardId("s1"), mock(JdbcOperations.class), mock(TransactionOperations.class)),
                mock(TaskLifecycleListener.class),
                null);

        assertThat(queueRunner, CoreMatchers.instanceOf(QueueRunnerInTransaction.class));
    }
}