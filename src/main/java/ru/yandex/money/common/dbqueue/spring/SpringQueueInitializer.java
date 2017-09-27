package ru.yandex.money.common.dbqueue.spring;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
import ru.yandex.money.common.dbqueue.init.QueueExecutionPool;
import ru.yandex.money.common.dbqueue.init.QueueRegistry;
import ru.yandex.money.common.dbqueue.settings.QueueConfig;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;


/**
 * Класс, собирающий конфигурацию очередей, региструющий её в {@link QueueRegistry}
 * и запускающий её через {@link QueueExecutionPool}.
 * <p>
 * Очереди стартуют после построения spring контекста и останавливаются при закрытии контекста.
 * Для использования достаточно создать bean в spring контексте.
 *
 * @author Oleg Kandaurov
 * @since 17.03.2016.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SpringQueueInitializer implements
        ApplicationListener<ContextRefreshedEvent>, DisposableBean {
    @Nonnull
    private final QueueRegistry queueRegistry;
    @Nonnull
    private final QueueExecutionPool executionPool;
    @Nonnull
    private final SpringQueueConfigContainer configContainer;
    @Nonnull
    private final SpringQueueCollector queueCollector;
    @Nonnull
    private final Collection<String> errorMessages = new ArrayList();

    /**
     * Конструктор
     *
     * @param configContainer настройки очередей
     * @param queueCollector  поставщик бинов, связанных с очередями
     * @param executionPool   менеджер запуска очередей
     */
    public SpringQueueInitializer(@Nonnull SpringQueueConfigContainer configContainer,
                                  @Nonnull SpringQueueCollector queueCollector,
                                  @Nonnull QueueExecutionPool executionPool) {
        this.configContainer = requireNonNull(configContainer);
        this.queueCollector = requireNonNull(queueCollector);
        this.executionPool = requireNonNull(executionPool);
        this.queueRegistry = requireNonNull(executionPool.getQueueRegistry());
    }

    private void init() {
        checkUnusedConfiguration();
        checkMissingConfiguration();
        throwIfHasErrors();
        validatePayloadType();
        throwIfHasErrors();
        wireQueueConfig();
        queueCollector.getShards().values().forEach(queueRegistry::registerShard);
        queueCollector.getTaskListeners().forEach(queueRegistry::registerTaskLifecycleListener);
        queueCollector.getThreadListeners().forEach(queueRegistry::registerThreadLifecycleListener);
        queueCollector.getExecutors().forEach(queueRegistry::registerExternalExecutor);
        queueRegistry.finishRegistration();
        queueCollector.getConsumers().values().forEach(SpringQueueConsumer::onInitialized);
        queueCollector.getProducers().values().forEach(SpringQueueProducer::onInitialized);
    }

    private void throwIfHasErrors() {
        if (!errorMessages.isEmpty()) {
            throw new IllegalArgumentException("unable to wire queue configuration:" + System.lineSeparator() +
                    errorMessages.stream().collect(Collectors.joining(System.lineSeparator())));
        }
    }

    private void wireQueueConfig() {
        queueCollector.getConsumers().forEach((queueId, consumer) -> {
            QueueConfig queueConfig = requireNonNull(configContainer.getQueueConfig(queueId).orElse(null));
            SpringTaskPayloadTransformer payloadTransformer = requireNonNull(
                    queueCollector.getTransformers().get(queueId));
            SpringQueueShardRouter shardRouter = requireNonNull(queueCollector.getShardRouters().get(queueId));

            consumer.setPayloadTransformer(payloadTransformer);
            consumer.setQueueConfig(queueConfig);
            consumer.setShardRouter(shardRouter);

            Collection<QueueShardId> routerShards = shardRouter.getShardsId();
            Map<QueueShardId, QueueDao> allShards = queueCollector.getShards();
            Map<QueueShardId, QueueDao> queueShards = routerShards.stream()
                    .filter(allShards::containsKey)
                    .map(allShards::get)
                    .collect(Collectors.toMap(QueueDao::getShardId, Function.identity()));
            SpringQueueProducer producer = requireNonNull(queueCollector.getProducers().get(queueId));
            producer.setPayloadTransformer(payloadTransformer);
            producer.setShardRouter(shardRouter);
            producer.setQueueConfig(queueConfig);
            producer.setShards(queueShards);
            queueRegistry.registerQueue(consumer, producer);
        });
    }

    private void checkMissingConfiguration() {
        queueCollector.getConsumers().forEach((queueId, consumer) -> {
            if (!configContainer.getQueueConfigs().containsKey(queueId)) {
                errorMessages.add("consumer config not found: queueId=" + queueId);
            }
            if (!queueCollector.getTransformers().containsKey(queueId)) {
                errorMessages.add("payload transformer not found: queueId=" + queueId);
            }
            if (!queueCollector.getShardRouters().containsKey(queueId)) {
                errorMessages.add("shard router not found: queueId=" + queueId);
            }
            if (!queueCollector.getProducers().containsKey(queueId)) {
                errorMessages.add("producer not found: queueId=" + queueId);
            }
        });
    }

    private void checkUnusedConfiguration() {
        queueCollector.getProducers().forEach((queueId, value) -> {
            if (!queueCollector.getConsumers().containsKey(queueId)) {
                errorMessages.add("unused producer: queueId=" + queueId);
            }
        });
        queueCollector.getShardRouters().forEach((queueId, value) -> {
            if (!queueCollector.getConsumers().containsKey(queueId)) {
                errorMessages.add("unused shard router: queueId=" + queueId);
            }
        });
        queueCollector.getTransformers().forEach((queueId, value) -> {
            if (!queueCollector.getConsumers().containsKey(queueId)) {
                errorMessages.add("unused transformer: queueId=" + queueId);
            }
        });
        configContainer.getQueueConfigs().forEach((queueId, value) -> {
            if (!queueCollector.getConsumers().containsKey(queueId)) {
                errorMessages.add("unused config: queueId=" + queueId);
            }
        });
    }

    private void validatePayloadType() {
        queueCollector.getConsumers().forEach((queueId, consumer) -> {
            SpringTaskPayloadTransformer payloadTransformer = queueCollector.getTransformers().get(queueId);
            SpringQueueShardRouter shardRouter = queueCollector.getShardRouters().get(queueId);
            SpringQueueProducer producer = queueCollector.getProducers().get(queueId);
            Class consumerPayloadClass = consumer.getPayloadClass();

            Class payloadTransformerClass = payloadTransformer.getPayloadClass();
            if (!Objects.equals(requireNonNull(consumerPayloadClass),
                    requireNonNull(payloadTransformerClass))) {
                errorMessages.add(String.format("payload transformer does not match consumer: " +
                                "queueId=%s, consumerClass=%s, transformerClass=%s",
                        consumer.getQueueId(), consumerPayloadClass.getCanonicalName(),
                        payloadTransformerClass.getCanonicalName()));
            }

            Class producerPayloadClass = producer.getPayloadClass();
            if (!Objects.equals(requireNonNull(consumerPayloadClass),
                    requireNonNull(producerPayloadClass))) {
                errorMessages.add(String.format("producer does not match consumer: " +
                                "queueId=%s, consumerClass=%s, producerClass=%s",
                        consumer.getQueueId(), consumerPayloadClass.getCanonicalName(),
                        producerPayloadClass.getCanonicalName()));
            }

            Class shardRouterPayloadClass = shardRouter.getPayloadClass();
            if (!Objects.equals(requireNonNull(consumerPayloadClass),
                    requireNonNull(shardRouterPayloadClass))) {
                errorMessages.add(String.format("shard router does not match consumer: " +
                                "queueId=%s, consumerClass=%s, shardRouterClass=%s",
                        consumer.getQueueId(), consumerPayloadClass.getCanonicalName(),
                        shardRouterPayloadClass.getCanonicalName()));
            }
        });
    }


    @Override
    public void destroy() throws Exception {
        executionPool.shutdown();
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        init();
        executionPool.init();
        executionPool.start();
    }
}
