package ru.yandex.money.common.dbqueue.api;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;
import ru.yandex.money.common.dbqueue.config.QueueShardId;

/**
 * @author Oleg Kandaurov
 * @since 10.08.2017
 */
public class QueueShardIdTest {

    @Test
    public void should_define_correct_equals_hashcode() throws Exception {
        EqualsVerifier.forClass(QueueShardId.class).verify();
    }

}