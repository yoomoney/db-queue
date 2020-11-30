package ru.yoomoney.tech.dbqueue.api;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;

/**
 * @author Oleg Kandaurov
 * @since 10.08.2017
 */
public class EnqueueParamsTest {

    @Test
    public void should_define_correct_equals_hashcode() throws Exception {
        EqualsVerifier.forClass(EnqueueParams.class).suppress(Warning.NONFINAL_FIELDS).verify();
    }
}