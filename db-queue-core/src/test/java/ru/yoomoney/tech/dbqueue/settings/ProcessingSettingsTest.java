package ru.yoomoney.tech.dbqueue.settings;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ProcessingSettingsTest {

    @Test
    public void should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(ProcessingSettings.class)
                .withIgnoredFields("observers")
                .suppress(Warning.NONFINAL_FIELDS)
                .usingGetClass().verify();
    }

    @Test
    public void should_set_value() {
        ProcessingSettings oldValue = ProcessingSettings.builder()
                .withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR).withThreadCount(1).build();
        ProcessingSettings newValue = ProcessingSettings.builder()
                .withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS).withThreadCount(0).build();
        Optional<String> diff = oldValue.setValue(newValue);
        assertThat(diff, equalTo(Optional.of("processingSettings(threadCount=0<1,processingMode=SEPARATE_TRANSACTIONS<USE_EXTERNAL_EXECUTOR)")));
        assertThat(oldValue, equalTo(newValue));
    }
}