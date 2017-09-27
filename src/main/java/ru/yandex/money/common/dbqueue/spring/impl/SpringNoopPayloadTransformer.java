package ru.yandex.money.common.dbqueue.spring.impl;

import ru.yandex.money.common.dbqueue.settings.QueueId;
import ru.yandex.money.common.dbqueue.spring.SpringTaskPayloadTransformer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Преобразователь данных задач, который не осуществляет преобразование данных.
 *
 * @author Oleg Kandaurov
 * @since 05.08.2017
 */
public class SpringNoopPayloadTransformer extends SpringTaskPayloadTransformer<String> {

    /**
     * Конструктор преобразователя данных задачи
     *
     * @param queueId идентификатор очереди
     */
    public SpringNoopPayloadTransformer(@Nonnull QueueId queueId) {
        super(queueId, String.class);
    }

    @Nullable
    @Override
    public String toObject(@Nullable String payload) {
        return payload;
    }

    @Nullable
    @Override
    public String fromObject(@Nullable String payload) {
        return payload;
    }
}
