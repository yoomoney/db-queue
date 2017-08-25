package ru.yandex.money.common.dbqueue.api.impl;

import ru.yandex.money.common.dbqueue.api.TaskPayloadTransformer;

import javax.annotation.Nullable;

/**
 * Пустой преобразователь данных очереди.
 * <p>
 * Используется в случае, когда преобразование данных не требуется.
 */
public final class NoopPayloadTransformer implements TaskPayloadTransformer<String> {

    private static final NoopPayloadTransformer INSTANCE = new NoopPayloadTransformer();

    /**
     * Получить инстанс преобразователя.
     *
     * @return инстанс преобразователя. Возвращается синглтон.
     */
    public static NoopPayloadTransformer getInstance() {
        return INSTANCE;
    }

    private NoopPayloadTransformer() {
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
