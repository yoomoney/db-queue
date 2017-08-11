package ru.yandex.money.common.dbqueue.example;

import ru.yandex.money.common.dbqueue.settings.QueueLocation;
import ru.yandex.money.common.dbqueue.spring.SpringPayloadTransformer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * @author Oleg Kandaurov
 * @since 12.07.2017
 */
public class CustomPayloadTransformer extends SpringPayloadTransformer<CustomPayload> {


    CustomPayloadTransformer(@Nonnull QueueLocation queueLocation,
                             @Nonnull Class<CustomPayload> payloadClass) {
        super(queueLocation, payloadClass);
    }

    @Nullable
    @Override
    public CustomPayload toObject(@Nullable String payload) {
        Objects.requireNonNull(payload);
        String[] parts = payload.split(":");
        return new CustomPayload(parts[0], parts[1]);
    }

    @Nullable
    @Override
    public String fromObject(@Nullable CustomPayload payload) {
        Objects.requireNonNull(payload);
        return String.format("%s:%s", payload.getType(), payload.getDescription());
    }

}
