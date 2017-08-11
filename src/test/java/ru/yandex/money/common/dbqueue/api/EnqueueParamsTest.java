package ru.yandex.money.common.dbqueue.api;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Oleg Kandaurov
 * @since 10.08.2017
 */
public class EnqueueParamsTest {

    @Test
    @Ignore
    public void should_define_correct_equals_hashcode() throws Exception {
        EqualsVerifier.forClass(EnqueueParams.class).verify();
    }
}