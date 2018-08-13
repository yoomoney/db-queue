package ru.yandex.money.common.dbqueue.spring.wiring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.money.common.dbqueue.api.EnqueueParams;
import ru.yandex.money.common.dbqueue.api.QueueConsumer;
import ru.yandex.money.common.dbqueue.api.QueueExternalExecutor;
import ru.yandex.money.common.dbqueue.api.QueueProducer;
import ru.yandex.money.common.dbqueue.api.QueueShard;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.api.QueueShardRouter;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.api.TaskExecutionResult;
import ru.yandex.money.common.dbqueue.api.TaskLifecycleListener;
import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;
import ru.yandex.money.common.dbqueue.api.TaskRecord;
import ru.yandex.money.common.dbqueue.api.ThreadLifecycleListener;
import ru.yandex.money.common.dbqueue.init.QueueExecutionPool;
import ru.yandex.money.common.dbqueue.init.QueueRegistry;
import ru.yandex.money.common.dbqueue.settings.ProcessingMode;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;
import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.settings.QueueSettings;
import ru.yandex.money.common.dbqueue.spring.SpringQueueCollector;
import ru.yandex.money.common.dbqueue.spring.SpringQueueConfigContainer;
import ru.yandex.money.common.dbqueue.spring.SpringQueueConsumer;
import ru.yandex.money.common.dbqueue.spring.SpringQueueExternalExecutor;
import ru.yandex.money.common.dbqueue.spring.SpringQueueInitializer;
import ru.yandex.money.common.dbqueue.spring.SpringSingleShardRouter;
import ru.yandex.money.common.dbqueue.spring.SpringTaskLifecycleListener;
import ru.yandex.money.common.dbqueue.spring.impl.SpringNoopPayloadTransformer;
import ru.yandex.money.common.dbqueue.spring.impl.SpringTransactionalProducer;
import ru.yandex.money.common.dbqueue.utils.QueueDatabaseInitializer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Oleg Kandaurov
 * @since 20.07.2017
 */
@Configuration
public class SpringLifecycleConfiguration {
    static final List<String> EVENTS = new ArrayList<>();

    private static final QueueId TEST_QUEUE_ID = new QueueId("lifecycle_queue");
    static final QueueLocation TEST_QUEUE_LOCATION =
            QueueLocation.builder().withTableName("lifecycle_table")
                    .withQueueId(TEST_QUEUE_ID).build();

    @Bean
    SpringQueueConfigContainer springQueueConfigContainer() {
        return new SpringQueueConfigContainer(Collections.singletonList(new QueueConfig(
                TEST_QUEUE_LOCATION,
                QueueSettings.builder()
                        .withBetweenTaskTimeout(Duration.ofMillis(20L))
                        .withNoTaskTimeout(Duration.ofMinutes(5L))
                        .withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR)
                        .build())));
    }

    @Bean
    ThreadLifecycleListener queueThreadLifecycleListener() {
        return new CustomThreadLifecycleListener();
    }

    @Bean
    TaskLifecycleListener defaultTaskLifecycleListener() {
        return new CustomTaskLifecycleListener("default");
    }

    @Bean
    SpringQueueCollector springQueueCollector() {
        return new SpringQueueCollector();
    }

    @Bean
    SpringQueueInitializer springQueueInitializer() {
        return new SpringQueueInitializer(springQueueConfigContainer(), springQueueCollector(),
                queueExecutionPool());
    }

    @Bean
    QueueExecutionPool queueExecutionPool() {
        return new QueueExecutionPool(new QueueRegistry(), defaultTaskLifecycleListener(), queueThreadLifecycleListener());
    }

    @Bean
    QueueShard queueShard() {
        return new QueueShard(new QueueShardId("shard1"), QueueDatabaseInitializer.getJdbcTemplate(),
                QueueDatabaseInitializer.getTransactionTemplate());
    }

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Bean
    QueueProducer<String> exampleProducer() {
        return new SpringTransactionalProducer<>(TEST_QUEUE_ID, String.class);
    }

    @Bean
    QueueConsumer<String> exampleQueue() {
        return new SpringQueueConsumer<String>(TEST_QUEUE_ID, String.class) {
            @Nonnull
            @Override
            public TaskExecutionResult execute(@Nonnull Task<String> task) {
                EVENTS.add("processing task");
                return TaskExecutionResult.finish();
            }
        };
    }

    @Bean
    TaskPayloadTransformer<String> exampleTransformer() {
        return new SpringNoopPayloadTransformer(TEST_QUEUE_ID) {
            @Nullable
            @Override
            public String toObject(@Nullable String payload) {
                EVENTS.add("transforming to object: " + payload);
                return super.toObject(payload);
            }

            @Nullable
            @Override
            public String fromObject(@Nullable String payload) {
                EVENTS.add("transforming from object: " + payload);
                return super.fromObject(payload);
            }
        };
    }

    @Bean
    QueueShardRouter<String> exampleShardRouter(QueueShard queueShard) {
        return new SpringSingleShardRouter<String>(TEST_QUEUE_ID, String.class, queueShard) {
            @Nonnull
            @Override
            public QueueShard resolveEnqueuingShard(@Nonnull EnqueueParams<String> enqueueParams) {
                EVENTS.add("resolved shard: " + queueShard.getShardId().asString());
                return queueShard;
            }
        };
    }

    @Bean
    QueueExternalExecutor exampleExternalExecutor() {
        return new SpringQueueExternalExecutor() {

            @Override
            public void shutdownQueueExecutor() {
                EVENTS.add("shutting down external executor");
            }

            @Override
            public void execute(Runnable command) {
                EVENTS.add("running in external pool");
                command.run();
            }

            @Nonnull
            @Override
            public QueueId getQueueId() {
                return TEST_QUEUE_ID;
            }
        };
    }

    @Bean
    TaskLifecycleListener exampleListener() {
        return new SpringTaskLifecycleListener(TEST_QUEUE_ID) {

            private final TaskLifecycleListener delegate = new CustomTaskLifecycleListener("example");

            @Override
            public void picked(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord, long pickTaskTime) {
                delegate.picked(shardId, location, taskRecord, pickTaskTime);
            }

            @Override
            public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord) {
                delegate.started(shardId, location, taskRecord);
            }

            @Override
            public void executed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord, @Nonnull TaskExecutionResult executionResult, long processTaskTime) {
                delegate.executed(shardId, location, taskRecord, executionResult, processTaskTime);
            }

            @Override
            public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord) {
                delegate.finished(shardId, location, taskRecord);
            }

            @Override
            public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord, @Nonnull Exception exc) {

            }
        };
    }

    public static class CustomThreadLifecycleListener implements ThreadLifecycleListener {

        @Override
        public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {
            EVENTS.add("queue started");
        }

        @Override
        public void executed(QueueShardId shardId, QueueLocation location, boolean taskProcessed, long threadBusyTime) {
            EVENTS.add("queue executed=" + taskProcessed);
        }

        @Override
        public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location) {
            EVENTS.add("queue finished");
        }

        @Override
        public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull Throwable exc) {
            EVENTS.add("queue crashed");
        }
    }

    public static class CustomTaskLifecycleListener implements TaskLifecycleListener {

        private final String listenerName;

        public CustomTaskLifecycleListener(String listenerName) {
            this.listenerName = listenerName;
        }

        @Override
        public void picked(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord, long pickTaskTime) {
            EVENTS.add("task picked on " + listenerName + " payload=" + taskRecord.getPayload());
        }

        @Override
        public void started(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord) {
            EVENTS.add("task started on " + listenerName + " payload=" + taskRecord.getPayload());
        }

        @Override
        public void executed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord,
                             @Nonnull TaskExecutionResult executionResult, long processTaskTime) {
            EVENTS.add("task executed on " + listenerName + " payload=" + taskRecord.getPayload());
        }

        @Override
        public void finished(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord) {
            EVENTS.add("task finished on " + listenerName + " payload=" + taskRecord.getPayload());
        }

        @Override
        public void crashed(@Nonnull QueueShardId shardId, @Nonnull QueueLocation location, @Nonnull TaskRecord taskRecord, @Nonnull Exception exc) {
            EVENTS.add("task crashed on " + listenerName + " payload=" + taskRecord.getPayload());
        }
    }
}
