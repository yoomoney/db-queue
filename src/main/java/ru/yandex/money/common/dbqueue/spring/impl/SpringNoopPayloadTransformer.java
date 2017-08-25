package ru.yandex.money.common.dbqueue.spring.impl;

import ru.yandex.money.common.dbqueue.settings.QueueLocation;
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
     * @param queueLocation местоположение очереди
     */
    public SpringNoopPayloadTransformer(@Nonnull QueueLocation queueLocation) {
        super(queueLocation, String.class);
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
