package ru.yandex.money.common.dbqueue.spring;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import ru.yandex.money.common.dbqueue.settings.QueueId;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Сборщик бинов, необходимых для конфигурирования очереди
 *
 * @author Oleg Kandaurov
 * @since 31.07.2017
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class SpringQueueCollector implements BeanPostProcessor, ApplicationListener<ContextRefreshedEvent> {
    private final Map<QueueId, SpringQueueProducer> producers = new LinkedHashMap<>();
    private final Map<QueueId, SpringQueueConsumer> queueConsumers = new LinkedHashMap<>();
    private final Map<QueueId, SpringTaskLifecycleListener> taskListeners = new LinkedHashMap<>();
    private final Map<QueueId, SpringThreadLifecycleListener> threadListeners = new LinkedHashMap<>();
    private final Map<QueueId, SpringQueueExternalExecutor> executors = new LinkedHashMap<>();
    private final Map<QueueId, SpringTaskPayloadTransformer> transformers = new LinkedHashMap<>();
    private final Map<QueueId, SpringQueueShardRouter> shardRouters = new LinkedHashMap<>();
    private final Collection<String> errorMessages = new ArrayList<>();

    private <T extends SpringQueueIdentifiable> void collectBeanIfPossible(
            Class<T> clazz, Object bean, Map<QueueId, T> storage, String beanName) {
        if (clazz.isAssignableFrom(bean.getClass())) {
            T obj = clazz.cast(bean);
            if (storage.containsKey(obj.getQueueId())) {
                errorMessages.add(String.format("duplicate bean: name=%s, class=%s, queueId=%s",
                        beanName, clazz.getSimpleName(), obj.getQueueId()));
                return;
            }
            storage.put(obj.getQueueId(), obj);
        }
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (!errorMessages.isEmpty()) {
            throw new IllegalArgumentException("unable to collect queue beans:" + System.lineSeparator() +
                    errorMessages.stream().collect(Collectors.joining(System.lineSeparator())));
        }
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        collectBeanIfPossible(SpringQueueConsumer.class, bean, queueConsumers, beanName);
        collectBeanIfPossible(SpringQueueProducer.class, bean, producers, beanName);
        collectBeanIfPossible(SpringTaskLifecycleListener.class, bean, taskListeners, beanName);
        collectBeanIfPossible(SpringThreadLifecycleListener.class, bean, threadListeners, beanName);
        collectBeanIfPossible(SpringQueueExternalExecutor.class, bean, executors, beanName);
        collectBeanIfPossible(SpringTaskPayloadTransformer.class, bean, transformers, beanName);
        collectBeanIfPossible(SpringQueueShardRouter.class, bean, shardRouters, beanName);
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    /**
     * Получить постановщики задач, найденные в spring контексте.
     *
     * @return Map: key - идентификатор очереди, value - постановщик задач данной очереди
     */
    @Nonnull
    Map<QueueId, SpringQueueProducer> getProducers() {
        return Collections.unmodifiableMap(producers);
    }

    /**
     * Получить обработчиков очереди, найденные в spring контексте.
     *
     * @return Map: key - идентификатор очереди, value - обработчик очереди
     */
    @Nonnull
    Map<QueueId, SpringQueueConsumer> getConsumers() {
        return Collections.unmodifiableMap(queueConsumers);
    }

    /**
     * Получить слушателей задач в данной очереди, найденных в spring контексте.
     *
     * @return Map: key - идентификатор очереди, value - слушатель задач данной очереди
     */
    @Nonnull
    Map<QueueId, SpringTaskLifecycleListener> getTaskListeners() {
        return Collections.unmodifiableMap(taskListeners);
    }

    /**
     * Получить слушателей потоков в данной очереди, найденных в spring контексте.
     *
     * @return Map: key - идентификатор очереди, value - слушатель потоков данной очереди
     */
    @Nonnull
    Map<QueueId, SpringThreadLifecycleListener> getThreadListeners() {
        return Collections.unmodifiableMap(threadListeners);
    }

    /**
     * Получить исполнителей задач, найденных в spring контексте.
     *
     * @return Map: key - идентификатор очереди, value - исполнитель задач данной очереди
     */
    @Nonnull
    Map<QueueId, SpringQueueExternalExecutor> getExecutors() {
        return Collections.unmodifiableMap(executors);
    }

    /**
     * Получить преобразователи данных задачи, найденные в spring контексте.
     *
     * @return Map: key - идентификатор очереди, value - преобразователь данных задачи для данной очереди
     */
    @Nonnull
    Map<QueueId, SpringTaskPayloadTransformer> getTransformers() {
        return Collections.unmodifiableMap(transformers);
    }

    /**
     * Получить правила шардирования, найденные в spring контексте.
     *
     * @return Map: key - идентификатор очереди, value - правила шардирования задач в очереди
     */
    @Nonnull
    Map<QueueId, SpringQueueShardRouter> getShardRouters() {
        return Collections.unmodifiableMap(shardRouters);
    }

}
