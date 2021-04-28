package ru.yoomoney.tech.dbqueue.settings;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

/**
 * @author Oleg Kandaurov
 * @since 27.09.2017
 */
public class QueueIdTest {

    @Test
    public void should_define_correct_equals_hashcode() throws Exception {
        EqualsVerifier.forClass(QueueId.class).verify();
    }
}