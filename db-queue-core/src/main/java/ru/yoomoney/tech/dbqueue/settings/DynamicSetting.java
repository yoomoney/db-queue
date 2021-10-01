package ru.yoomoney.tech.dbqueue.settings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * TODO: javadoc
 *
 * @param <T> javadoc
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
abstract class DynamicSetting<T> {

    private static final Logger log = LoggerFactory.getLogger(DynamicSetting.class);

    @Nonnull
    private final List<BiConsumer<T, T>> observers = new CopyOnWriteArrayList<>();

    @Nonnull
    protected abstract String getName();

    @Nonnull
    protected abstract BiFunction<T, T, String> getDiffEvaluator();

    @Nonnull
    protected abstract T getThis();

    protected abstract void copyFields(@Nonnull T newValue);

    public final Optional<String> setValue(@Nonnull T newValue) {
        T oldValue = getThis();
        try {
            Objects.requireNonNull(newValue, getName() + " must not be null");
            if (newValue.equals(oldValue)) {
                return Optional.empty();
            }
            observers.forEach(observer -> observer.accept(oldValue, newValue));
            String diff = getDiffEvaluator().apply(oldValue, newValue);
            copyFields(newValue);
            return Optional.of(diff);
        } catch (RuntimeException exc) {
            log.error("Cannot apply new setting: name={}, oldValue={}, newValue={}",
                    getName(), oldValue, newValue, exc);
            return Optional.empty();
        }
    }

    public final void registerObserver(BiConsumer<T, T> observer) {
        observers.add(observer);
    }

}
