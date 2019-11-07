package ru.yandex.money.common.dbqueue.settings;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * @author Oleg Kandaurov
 * @since 10.08.2017
 */
public class QueueLocationTest {

    @Test
    public void should_define_correct_equals_hashcode() throws Exception {
        EqualsVerifier.forClass(QueueLocation.class).verify();
    }

    @Test
    public void should_filter_special_chars() {
        Assert.assertThat(QueueLocation.builder().withQueueId(new QueueId("1"))
                .withTableName(" l !@#$%^&*()._+-=1\n;'][{}").build().getTableName(), equalTo("l._1"));
    }

}