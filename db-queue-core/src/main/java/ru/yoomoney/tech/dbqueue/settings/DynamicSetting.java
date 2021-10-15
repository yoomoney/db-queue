package ru.yoomoney.tech.dbqueue.settings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Base class for dynamic settings.
 * <p>
 * Use it when you need track changes in some setting.
 *
 * @param <T> type of setting
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
abstract class DynamicSetting<T> {

    private static final Logger log = LoggerFactory.getLogger(DynamicSetting.class);

    @Nonnull
    private final Collection<BiConsumer<T, T>> observers = new CopyOnWriteArrayList<>();

    /**
     * Name of setting
     *
     * @return name
     */
    @Nonnull
    protected abstract String getName();

    /**
     * Function evaluates difference between new and old value.
     * 1st argument - old value, 2nd argument - new value.
     *
     * @return difference between two values
     */
    @Nonnull
    protected abstract BiFunction<T, T, String> getDiffEvaluator();

    /**
     * Return typed reference of current object
     *
     * @return current object
     */
    @Nonnull
    protected abstract T getThis();

    /**
     * Copy fields of new object to current object.
     *
     * @param newValue new value
     */
    protected abstract void copyFields(@Nonnull T newValue);

    /**
     * Sets new value for current setting.
     * Notifies observer when property is changed.
     *
     * @param newValue new value for setting
     * @return diff between old value and new value. Returns empty object when no changes detected.
     * @see DynamicSetting#registerObserver(BiConsumer)
     */
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

    /**
     * Register observer to track setting changes.
     *
     * @param observer consumer which will be notified on property change.
     *                 1st argument - old value, 2nd argument - new value
     */
    public final void registerObserver(BiConsumer<T, T> observer) {
        observers.add(observer);
    }

}
