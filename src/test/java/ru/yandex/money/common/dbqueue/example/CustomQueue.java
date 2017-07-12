package ru.yandex.money.common.dbqueue.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.money.common.dbqueue.api.QueueAction;
import ru.yandex.money.common.dbqueue.api.Task;
import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.spring.SpringQueue;

import javax.annotation.Nonnull;

/**
 * @author Oleg Kandaurov
 * @since 12.07.2017
 */
public class CustomQueue extends SpringQueue<CustomPayload> {

    private static final Logger log = LoggerFactory.getLogger(CustomQueue.class);

    CustomQueue(@Nonnull QueueLocation queueLocation, @Nonnull Class<CustomPayload> payloadClass) {
        super(queueLocation, payloadClass);
    }

    @Nonnull
    @Override
    public QueueAction execute(@Nonnull Task<CustomPayload> task) {
        log.info("processed task: payload={}", task.getPayload());
        return QueueAction.finish();
    }

}
