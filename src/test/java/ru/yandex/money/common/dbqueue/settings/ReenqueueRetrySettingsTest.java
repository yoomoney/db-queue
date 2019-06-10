package ru.yandex.money.common.dbqueue.settings;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ReenqueueRetrySettingsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(ReenqueueRetrySettings.class).usingGetClass().verify();
    }

    @Test
    public void should_throw_exception_when_fixed_delay_not_set() {
        ReenqueueRetrySettings settings = ReenqueueRetrySettings.builder(ReenqueueRetryType.FIXED)
                .build();

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("fixed delay is null");

        settings.getFixedDelayOrThrow();
    }

    @Test
    public void should_throw_exception_when_sequential_plan_not_set() {
        ReenqueueRetrySettings settings = ReenqueueRetrySettings.builder(ReenqueueRetryType.SEQUENTIAL)
                .build();

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("sequential plan is null");

        settings.getSequentialPlanOrThrow();
    }
}
