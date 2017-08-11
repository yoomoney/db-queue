package ru.yandex.money.common.dbqueue.settings;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

/**
 * @author Oleg Kandaurov
 * @since 10.08.2017
 */
public class QueueLocationTest {

    @Test
    public void should_define_correct_equals_hashcode() throws Exception {
        EqualsVerifier.forClass(QueueLocation.class).verify();
    }

}