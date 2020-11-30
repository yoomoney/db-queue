package ru.yoomoney.tech.dbqueue.api.impl;

import ru.yoomoney.tech.dbqueue.api.TaskPayloadTransformer;

import javax.annotation.Nullable;

/**
 * Default payload transformer, which performs no transformation
 * and returns the same string as in the raw payload.
 * <p>
 * Use where no transformation required.
 */
public final class NoopPayloadTransformer implements TaskPayloadTransformer<String> {

    private static final NoopPayloadTransformer INSTANCE = new NoopPayloadTransformer();

    /**
     * Get payload transformer instance.
     *
     * @return Singleton of transformer.
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
