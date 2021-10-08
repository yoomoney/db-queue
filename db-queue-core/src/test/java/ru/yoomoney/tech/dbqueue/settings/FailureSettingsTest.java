package ru.yoomoney.tech.dbqueue.settings;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;

import java.time.Duration;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class FailureSettingsTest {

    @Test
    public void should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(FailureSettings.class)
                .withIgnoredFields("observers")
                .suppress(Warning.NONFINAL_FIELDS)
                .usingGetClass().verify();
    }

    @Test
    public void should_set_value() {
        FailureSettings oldValue = FailureSettings.builder()
                .withRetryType(FailRetryType.GEOMETRIC_BACKOFF)
                .withRetryInterval(Duration.ofSeconds(1)).build();
        FailureSettings newValue = FailureSettings.builder()
                .withRetryType(FailRetryType.ARITHMETIC_BACKOFF)
                .withRetryInterval(Duration.ofSeconds(5)).build();
        Optional<String> diff = oldValue.setValue(newValue);
        assertThat(diff, equalTo(Optional.of("failureSettings(retryType=ARITHMETIC_BACKOFF<GEOMETRIC_BACKOFF,retryInterval=PT5S<PT1S)")));
        assertThat(oldValue, equalTo(newValue));
    }
}