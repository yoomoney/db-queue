package ru.yoomoney.tech.dbqueue.api.impl;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.api.EnqueueResult;
import ru.yoomoney.tech.dbqueue.api.QueueProducer;
import ru.yoomoney.tech.dbqueue.config.QueueShardId;
import ru.yoomoney.tech.dbqueue.settings.QueueId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MonitoringQueueProducerTest {

    public static final Path LOG_PATH = Paths.get("target/monitoring-queue-producer.log");

    @Before
    public void setUp() throws Exception {
        if (Files.exists(LOG_PATH)) {
            Files.write(LOG_PATH, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        }
    }

    @Test
    public void should_invoke_monitoring_callback_and_print_logs() throws IOException {
        Clock clock = mock(Clock.class);

        EnqueueResult expectedResult = EnqueueResult.builder().withShardId(new QueueShardId("main")).withEnqueueId(1L).build();
        QueueProducer<String> producer = mock(QueueProducer.class);

        MonitoringQueueProducer<String> monitoringProducer = new MonitoringQueueProducer<>(producer, new QueueId("test"),
                (enqueueResult, time) -> {
                    assertThat(enqueueResult, equalTo(expectedResult));
                    assertThat(time, equalTo(4L));
                }, clock);

        when(producer.enqueue(EnqueueParams.create("1"))).thenReturn(expectedResult);
        when(clock.millis()).thenAnswer(new Answer() {
            private int count = 0;

            public Object answer(InvocationOnMock invocation) {
                count++;
                if (count == 1) {
                    return 1L;
                } else if (count == 2) {
                    return 5L;
                } else {
                    throw new IllegalStateException();
                }
            }
        });

        EnqueueResult actualResult = monitoringProducer.enqueue(EnqueueParams.create("1"));
        assertThat(actualResult, equalTo(expectedResult));

        List<String> logFile = Files.readAllLines(LOG_PATH);
        assertThat(logFile, equalTo(Arrays.asList(
                "INFO  [MonitoringQueueProducer] enqueuing task: queue=test, delay=PT0S",
                "INFO  [MonitoringQueueProducer] task enqueued: id=1, queueShardId=main")));
    }
}