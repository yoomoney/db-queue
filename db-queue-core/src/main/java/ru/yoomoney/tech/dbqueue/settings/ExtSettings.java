package ru.yoomoney.tech.dbqueue.settings;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.BiFunction;

/**
 * Additional custom settings
 */
public class ExtSettings extends DynamicSetting<ExtSettings> {

    private Map<String, String> extSettings;

    ExtSettings(@Nonnull Map<String, String> extSettings) {
        this.extSettings = Objects.requireNonNull(extSettings, "extSettings must not be null");
    }

    @Nonnull
    @Override
    protected String getName() {
        return "extSettings";
    }

    @Nonnull
    @Override
    protected BiFunction<ExtSettings, ExtSettings, String> getDiffEvaluator() {
        return (oldValue, newValue) -> {
            Collection<String> sameEntries = new LinkedHashSet<>(newValue.extSettings.keySet());
            sameEntries.retainAll(oldValue.extSettings.keySet());

            Collection<String> entriesInNew = new LinkedHashSet<>(newValue.extSettings.keySet());
            entriesInNew.removeAll(oldValue.extSettings.keySet());

            Collection<String> entriesInOld = new LinkedHashSet<>(oldValue.extSettings.keySet());
            entriesInOld.removeAll(newValue.extSettings.keySet());

            StringJoiner diff = new StringJoiner(",", getName() + '(', ")");
            sameEntries.forEach(key -> diff.add(key + '=' + newValue.extSettings.get(key) + '<' + oldValue.extSettings.get(key)));
            entriesInNew.forEach(key -> diff.add(key + '=' + newValue.extSettings.get(key) + '<' + null));
            entriesInOld.forEach(key -> diff.add(key + '=' + null + '<' + oldValue.extSettings.get(key)));
            return diff.toString();
        };
    }

    @Nonnull
    @Override
    protected ExtSettings getThis() {
        return this;
    }

    @Override
    protected void copyFields(@Nonnull ExtSettings newValue) {
        this.extSettings = newValue.extSettings;
    }

    /**
     * Get {@linkplain Duration} value of additional queue property.
     *
     * @param settingName Name of the property.
     * @return Property value.
     */
    @Nonnull
    public Duration getDurationProperty(@Nonnull String settingName) {
        return Duration.parse(getProperty(settingName));
    }

    /**
     * Get string value of additional queue property.
     *
     * @param settingName Name of the property.
     * @return Property value.
     */
    @Nonnull
    public String getProperty(@Nonnull String settingName) {
        Objects.requireNonNull(settingName);
        return Objects.requireNonNull(extSettings.get(settingName),
                String.format("null values are not allowed: settingName=%s", settingName));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ExtSettings that = (ExtSettings) obj;
        return Objects.equals(extSettings, that.extSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(extSettings);
    }


    @Override
    public String toString() {
        return extSettings.toString();
    }

    /**
     * Create a new builder for ext settings.
     *
     * @return A new builder for ext settings.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for ext settings.
     */
    public static class Builder {
        private Map<String, String> extSettings;

        private Builder() {
        }

        public Builder withSettings(@Nonnull Map<String, String> extSettings) {
            this.extSettings = extSettings;
            return this;
        }

        public ExtSettings build() {
            return new ExtSettings(extSettings);
        }
    }
}
