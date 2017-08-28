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
        queueCollector.getConsumers().forEach((location, consumer) -> {
            QueueConfig queueConfig = requireNonNull(configContainer.getQueueConfig(location).orElse(null));
            SpringTaskPayloadTransformer payloadTransformer = requireNonNull(
                    queueCollector.getTransformers().get(location));
            SpringQueueShardRouter shardRouter = requireNonNull(queueCollector.getShardRouters().get(location));

            consumer.setPayloadTransformer(payloadTransformer);
            consumer.setQueueConfig(queueConfig);
            consumer.setShardRouter(shardRouter);

            Collection<QueueShardId> routerShards = shardRouter.getShardsId();
            Map<QueueShardId, QueueDao> allShards = queueCollector.getShards();
            Map<QueueShardId, QueueDao> queueShards = routerShards.stream()
                    .filter(allShards::containsKey)
                    .map(allShards::get)
                    .collect(Collectors.toMap(QueueDao::getShardId, Function.identity()));
            SpringQueueProducer producer = requireNonNull(queueCollector.getProducers().get(location));
            producer.setPayloadTransformer(payloadTransformer);
            producer.setShardRouter(shardRouter);
            producer.setQueueConfig(queueConfig);
            producer.setShards(queueShards);
            queueRegistry.registerQueue(consumer, producer);
        });
    }

    private void checkMissingConfiguration() {
        queueCollector.getConsumers().forEach((location, consumer) -> {
            if (!configContainer.getQueueConfigs().containsKey(location)) {
                errorMessages.add("consumer config not found: location=" + location);
            }
            if (!queueCollector.getTransformers().containsKey(location)) {
                errorMessages.add("payload transformer not found: location=" + location);
            }
            if (!queueCollector.getShardRouters().containsKey(location)) {
                errorMessages.add("shard router not found: location=" + location);
            }
            if (!queueCollector.getProducers().containsKey(location)) {
                errorMessages.add("producer not found: location=" + location);
            }
        });
    }

    private void checkUnusedConfiguration() {
        queueCollector.getProducers().forEach((location, value) -> {
            if (!queueCollector.getConsumers().containsKey(location)) {
                errorMessages.add("unused producer: location=" + location);
            }
        });
        queueCollector.getShardRouters().forEach((location, value) -> {
            if (!queueCollector.getConsumers().containsKey(location)) {
                errorMessages.add("unused shard router: location=" + location);
            }
        });
        queueCollector.getTransformers().forEach((location, value) -> {
            if (!queueCollector.getConsumers().containsKey(location)) {
                errorMessages.add("unused transformer: location=" + location);
            }
        });
        configContainer.getQueueConfigs().forEach((location, value) -> {
            if (!queueCollector.getConsumers().containsKey(location)) {
                errorMessages.add("unused config: location=" + location);
            }
        });
    }

    private void validatePayloadType() {
        queueCollector.getConsumers().forEach((location, consumer) -> {
            SpringTaskPayloadTransformer payloadTransformer = queueCollector.getTransformers().get(location);
            SpringQueueShardRouter shardRouter = queueCollector.getShardRouters().get(location);
            SpringQueueProducer producer = queueCollector.getProducers().get(location);
            Class consumerPayloadClass = consumer.getPayloadClass();

            Class payloadTransformerClass = payloadTransformer.getPayloadClass();
            if (!Objects.equals(requireNonNull(consumerPayloadClass),
                    requireNonNull(payloadTransformerClass))) {
                errorMessages.add(String.format("payload transformer does not match consumer: " +
                                "location=%s, consumerClass=%s, transformerClass=%s",
                        consumer.getQueueLocation(), consumerPayloadClass.getCanonicalName(),
                        payloadTransformerClass.getCanonicalName()));
            }

            Class producerPayloadClass = producer.getPayloadClass();
            if (!Objects.equals(requireNonNull(consumerPayloadClass),
                    requireNonNull(producerPayloadClass))) {
                errorMessages.add(String.format("producer does not match consumer: " +
                                "location=%s, consumerClass=%s, producerClass=%s",
                        consumer.getQueueLocation(), consumerPayloadClass.getCanonicalName(),
                        producerPayloadClass.getCanonicalName()));
            }

            Class shardRouterPayloadClass = shardRouter.getPayloadClass();
            if (!Objects.equals(requireNonNull(consumerPayloadClass),
                    requireNonNull(shardRouterPayloadClass))) {
                errorMessages.add(String.format("shard router does not match consumer: " +
                                "location=%s, consumerClass=%s, shardRouterClass=%s",
                        consumer.getQueueLocation(), consumerPayloadClass.getCanonicalName(),
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
