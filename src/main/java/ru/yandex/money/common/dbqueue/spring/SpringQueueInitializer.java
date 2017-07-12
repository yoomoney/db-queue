package ru.yandex.money.common.dbqueue.spring;

import org.springframework.beans.factory.InitializingBean;
import ru.yandex.money.common.dbqueue.api.QueueShardId;
import ru.yandex.money.common.dbqueue.dao.QueueDao;
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
 * Класс, собирающий спринговую конфигурацию, и региструющий её в {@link QueueRegistry}
 *
 * @author Oleg Kandaurov
 * @since 17.03.2016.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SpringQueueInitializer implements InitializingBean {

    @Nonnull
    private final QueueRegistry queueRegistry;
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
     * @param queueCollector поставщик бинов, связанных с очередями
     * @param queueRegistry хранилище очередей
     */
    public SpringQueueInitializer(@Nonnull SpringQueueConfigContainer configContainer,
                                  @Nonnull SpringQueueCollector queueCollector,
                                  @Nonnull QueueRegistry queueRegistry) {
        this.configContainer = requireNonNull(configContainer);
        this.queueCollector = requireNonNull(queueCollector);
        this.queueRegistry = requireNonNull(queueRegistry);
    }

    private void init() {
        checkUnusedConfiguration();
        checkMissingConfiguration();
        throwIfHasErrors();
        validatePayloadType();
        throwIfHasErrors();
        wireQueueConfig();
        queueCollector.getShards().values().forEach(queueRegistry::registerShard);
        queueCollector.getListeners().forEach(queueRegistry::registerTaskLifecycleListener);
        queueCollector.getExecutors().forEach(queueRegistry::registerExternalExecutor);
        queueRegistry.finishRegistration();
    }

    private void throwIfHasErrors() {
        if (!errorMessages.isEmpty()) {
            throw new IllegalArgumentException("unable to wire queue configuration:" + System.lineSeparator() +
                    errorMessages.stream().collect(Collectors.joining(System.lineSeparator())));
        }
    }

    private void wireQueueConfig() {
        queueCollector.getQueues().forEach((location, queue) -> {
            QueueConfig queueConfig = requireNonNull(configContainer.getQueueConfig(location).orElse(null));
            SpringPayloadTransformer payloadTransformer = requireNonNull(
                    queueCollector.getTransformers().get(location));
            SpringShardRouter shardRouter = requireNonNull(queueCollector.getShardRouters().get(location));

            queue.setPayloadTransformer(payloadTransformer);
            queue.setQueueConfig(queueConfig);
            queue.setShardRouter(shardRouter);

            Collection<QueueShardId> routerShards = shardRouter.getShardsId();
            Map<QueueShardId, QueueDao> allShards = queueCollector.getShards();
            Map<QueueShardId, QueueDao> queueShards = routerShards.stream()
                    .filter(allShards::containsKey)
                    .map(allShards::get)
                    .collect(Collectors.toMap(QueueDao::getShardId, Function.identity()));
            SpringEnqueuer enqueuer = requireNonNull(queueCollector.getEnqueuers().get(location));
            enqueuer.setPayloadTransformer(payloadTransformer);
            enqueuer.setShardRouter(shardRouter);
            enqueuer.setQueueConfig(queueConfig);
            enqueuer.setShards(queueShards);


            queueRegistry.registerQueue(queue, enqueuer);
        });
    }

    private void checkMissingConfiguration() {
        queueCollector.getQueues().forEach((location, queue) -> {
            if (!configContainer.getQueueConfigs().containsKey(location)) {
                errorMessages.add("queue config not found: location=" + location);
            }
            if (!queueCollector.getTransformers().containsKey(location)) {
                errorMessages.add("payload transformer not found: location=" + location);
            }
            if (!queueCollector.getShardRouters().containsKey(location)) {
                errorMessages.add("shard router not found: location=" + location);
            }
            if (!queueCollector.getEnqueuers().containsKey(location)) {
                errorMessages.add("enqueuer not found: location=" + location);
            }
        });
    }

    private void checkUnusedConfiguration() {
        queueCollector.getEnqueuers().forEach((location, value) -> {
            if (!queueCollector.getQueues().containsKey(location)) {
                errorMessages.add("unused enqueuer: location=" + location);
            }
        });
        queueCollector.getShardRouters().forEach((location, value) -> {
            if (!queueCollector.getQueues().containsKey(location)) {
                errorMessages.add("unused shard router: location=" + location);
            }
        });
        queueCollector.getTransformers().forEach((location, value) -> {
            if (!queueCollector.getQueues().containsKey(location)) {
                errorMessages.add("unused transformer: location=" + location);
            }
        });
        configContainer.getQueueConfigs().forEach((location, value) -> {
            if (!queueCollector.getQueues().containsKey(location)) {
                errorMessages.add("unused config: location=" + location);
            }
        });
    }

    private void validatePayloadType() {
        queueCollector.getQueues().forEach((location, queue) -> {
            SpringPayloadTransformer payloadTransformer = queueCollector.getTransformers().get(location);
            SpringShardRouter shardRouter = queueCollector.getShardRouters().get(location);
            SpringEnqueuer enqueuer = queueCollector.getEnqueuers().get(location);
            Class queuePayloadClass = queue.getPayloadClass();

            Class payloadTransformerClass = payloadTransformer.getPayloadClass();
            if (!Objects.equals(requireNonNull(queuePayloadClass),
                    requireNonNull(payloadTransformerClass))) {
                errorMessages.add(String.format("payload transformer does not match queue: " +
                                "location=%s, queueClass=%s, transformerClass=%s",
                        queue.getQueueLocation(), queuePayloadClass.getCanonicalName(),
                        payloadTransformerClass.getCanonicalName()));
            }

            Class enqueuerPayloadClass = enqueuer.getPayloadClass();
            if (!Objects.equals(requireNonNull(queuePayloadClass),
                    requireNonNull(enqueuerPayloadClass))) {
                errorMessages.add(String.format("enqueuer does not match queue: " +
                                "location=%s, queueClass=%s, enqueuerClass=%s",
                        queue.getQueueLocation(), queuePayloadClass.getCanonicalName(),
                        enqueuerPayloadClass.getCanonicalName()));
            }

            Class shardRouterPayloadClass = shardRouter.getPayloadClass();
            if (!Objects.equals(requireNonNull(queuePayloadClass),
                    requireNonNull(shardRouterPayloadClass))) {
                errorMessages.add(String.format("shard router does not match queue: " +
                                "location=%s, queueClass=%s, shardRouterClass=%s",
                        queue.getQueueLocation(), queuePayloadClass.getCanonicalName(),
                        shardRouterPayloadClass.getCanonicalName()));
            }
        });
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }
}
