package ru.yandex.money.common.dbqueue.spring;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import ru.yandex.money.common.dbqueue.init.QueueExecutionPool;

/**
 * Класс, обеспечивающий запуск обработки задач в очереди, в spring конфигурации.
 * <p>
 * Очереди стартуют после построения spring контекста и останавливаются при закрытии контекста.
 * Для использования достаточно создать bean в spring контексте.
 *
 * @author Oleg Kandaurov
 * @since 02.08.2017
 */
public class SpringQueueStarter implements ApplicationListener<ContextRefreshedEvent>, DisposableBean {

    private final QueueExecutionPool queueExecutionPool;

    /**
     * Конструктор
     *
     * @param queueExecutionPool класс для запуска и останова очередей
     */
    public SpringQueueStarter(QueueExecutionPool queueExecutionPool) {
        this.queueExecutionPool = queueExecutionPool;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        queueExecutionPool.init();
        queueExecutionPool.start();
    }

    @Override
    public void destroy() throws Exception {
        queueExecutionPool.shutdown();
    }
}
