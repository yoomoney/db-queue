package ru.yoomoney.tech.dbqueue.settings;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ExtSettingsTest {

    @Test
    public void should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(ExtSettings.class)
                .withIgnoredFields("observers")
                .suppress(Warning.NONFINAL_FIELDS)
                .usingGetClass().verify();
    }

    @Test
    public void should_set_value() {
        Map<String, String> oldMap = new LinkedHashMap<>();
        oldMap.put("same", "1");
        oldMap.put("old", "0");
        Map<String, String> newMap = new LinkedHashMap<>();
        newMap.put("same", "2");
        newMap.put("new", "3");
        ExtSettings oldValue = ExtSettings.builder().withSettings(oldMap).build();
        ExtSettings newValue = ExtSettings.builder().withSettings(newMap).build();
        Optional<String> diff = oldValue.setValue(newValue);
        assertThat(diff, equalTo(Optional.of("extSettings(same=2<1,new=3<null,old=null<0)")));
        assertThat(oldValue, equalTo(newValue));
    }
}