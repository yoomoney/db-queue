package ru.yandex.money.common.dbqueue.config;

import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author Oleg Kandaurov
 * @since 16.10.2019
 */

public class QueueTableSchemaTest {

    @Test
    public void should_filter_special_chars() {
        QueueTableSchema schema = QueueTableSchema.builder()
                .withQueueNameField("qn !@#$%^&*()_+-=1\n;'][{}")
                .withPayloadField("pl !@#$%^&*()_+-=1\n;'][{}")
                .withCreatedAtField("ct !@#$%^&*()_+-=1\n;'][{}")
                .withNextProcessAtField("pt !@#$%^&*()_+-=1\n;'][{}")
                .withAttemptField("at !@#$%^&*()_+-=1\n;'][{}")
                .withReenqueueAttemptField("rat !@#$%^&*()_+-=1\n;'][{}")
                .withTotalAttemptField("tat !@#$%^&*()_+-=1\n;'][{}")
                .withExtFields(Collections.singletonList("tr !@#$%^&*()_+-=1\n;'][{}"))
                .build();
        assertThat(schema.getQueueNameField(), equalTo("qn_1"));
        assertThat(schema.getPayloadField(), equalTo("pl_1"));
        assertThat(schema.getCreatedAtField(), equalTo("ct_1"));
        assertThat(schema.getNextProcessAtField(), equalTo("pt_1"));
        assertThat(schema.getAttemptField(), equalTo("at_1"));
        assertThat(schema.getReenqueueAttemptField(), equalTo("rat_1"));
        assertThat(schema.getTotalAttemptField(), equalTo("tat_1"));
        assertThat(schema.getExtFields().get(0), equalTo("tr_1"));
    }

}
