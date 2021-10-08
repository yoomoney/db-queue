package ru.yoomoney.tech.dbqueue.settings;

import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class DynamicSettingTest {

    @Test
    public void should_not_invoke_observer_when_no_changes() {
        AtomicBoolean observerInvoked = new AtomicBoolean(false);
        SimpleDynamicSetting oldSetting = new SimpleDynamicSetting("old");
        oldSetting.registerObserver((oldVal, newVal) -> {
            observerInvoked.set(true);
        });
        SimpleDynamicSetting newSetting = new SimpleDynamicSetting("old");
        Optional<String> diff = oldSetting.setValue(newSetting);
        assertThat(diff, equalTo(Optional.empty()));
        assertThat(observerInvoked.get(), equalTo(false));
        assertThat(oldSetting, equalTo(newSetting));
    }

    @Test
    public void should_invoke_observer_when_setting_changed() {
        AtomicBoolean observerInvoked = new AtomicBoolean(false);
        SimpleDynamicSetting oldSetting = new SimpleDynamicSetting("old");
        oldSetting.registerObserver((oldVal, newVal) -> {
            observerInvoked.set(true);
        });
        SimpleDynamicSetting newSetting = new SimpleDynamicSetting("new");
        Optional<String> diff = oldSetting.setValue(newSetting);
        assertThat(diff, equalTo(Optional.of("new<old")));
        assertThat(observerInvoked.get(), equalTo(true));
        assertThat(oldSetting, equalTo(newSetting));
    }

    @Test
    public void should_not_update_setting_when_observer_fails() {
        SimpleDynamicSetting oldSetting = new SimpleDynamicSetting("old");
        oldSetting.registerObserver((oldVal, newVal) -> {
            throw new RuntimeException("exc");
        });
        SimpleDynamicSetting newSetting = new SimpleDynamicSetting("new");
        Optional<String> diff = oldSetting.setValue(newSetting);
        assertThat(diff, equalTo(Optional.empty()));
        assertThat(oldSetting, not(equalTo(newSetting)));
    }

    private static class SimpleDynamicSetting extends DynamicSetting<SimpleDynamicSetting> {

        private String text;

        private SimpleDynamicSetting(String text) {
            this.text = text;
        }

        @Nonnull
        @Override
        protected String getName() {
            return "simple";
        }

        @Nonnull
        @Override
        protected BiFunction<SimpleDynamicSetting, SimpleDynamicSetting, String> getDiffEvaluator() {
            return (oldVal, newVal) -> newVal.text + '<' + oldVal.text;
        }

        @Nonnull
        @Override
        protected SimpleDynamicSetting getThis() {
            return this;
        }

        @Override
        protected void copyFields(@Nonnull SimpleDynamicSetting newValue) {
            this.text = newValue.text;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SimpleDynamicSetting that = (SimpleDynamicSetting) o;
            return Objects.equals(text, that.text);
        }

        @Override
        public int hashCode() {
            return Objects.hash(text);
        }
    }
}