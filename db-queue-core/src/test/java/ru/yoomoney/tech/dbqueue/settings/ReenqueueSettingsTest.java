package ru.yoomoney.tech.dbqueue.settings;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ReenqueueSettingsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(ReenqueueSettings.class)
                .withIgnoredFields("observers")
                .suppress(Warning.NONFINAL_FIELDS)
                .usingGetClass().verify();
    }

    @Test
    public void should_throw_exception_when_fixed_delay_not_set() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("fixedDelay must not be empty when retryType=FIXED");
        ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.FIXED).build();
    }

    @Test
    public void should_throw_exception_when_sequential_plan_not_set() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("sequentialPlan must not be empty when retryType=SEQUENTIAL");
        ReenqueueSettings.builder().withRetryType(ReenqueueRetryType.SEQUENTIAL)
                .build();

    }
}
