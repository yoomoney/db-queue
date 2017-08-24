package ru.yandex.money.common.dbqueue.settings;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Oleg Kandaurov
 * @since 22.08.2017
 */
public class QueueConfigsReaderTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private QueueConfig createConfig(String tableName, String queueName, QueueSettings settings) {
        return new QueueConfig(new QueueLocation(tableName, queueName), settings);
    }

    @Test
    public void should_read_simple_config() throws Exception {
        QueueConfigsReader queueConfigsReader = new QueueConfigsReader("q");
        Collection<QueueConfig> configs = queueConfigsReader.parse(fileSystem.write(
                "q.testQueue.table=foo",
                "q.testQueue.between-task-timeout=PT0.1S",
                "q.testQueue.no-task-timeout=PT5S"));
        assertThat(configs, equalTo(Collections.singletonList(
                createConfig("foo", "testQueue",
                        QueueSettings.builder().withBetweenTaskTimeout(Duration.ofMillis(100L))
                                .withNoTaskTimeout(Duration.ofSeconds(5L)).build()))));
    }

    @Test
    public void should_read_full_config() throws Exception {
        QueueConfigsReader queueConfigsReader = new QueueConfigsReader("q");
        Collection<QueueConfig> configs = queueConfigsReader.parse(fileSystem.write(
                "q.testQueue.table=foo",
                "q.testQueue.between-task-timeout=PT0.1S",
                "q.testQueue.no-task-timeout=PT5S",
                "q.testQueue.fatal-crash-timeout=PT1H",
                "q.testQueue.thread-count=3",
                "q.testQueue.retry-type=fixed-interval",
                "q.testQueue.processing-mode=use-external-executor",
                "q.testQueue.additional-settings.retry-fixed-interval-delay=PT3H",
                "q.testQueue.additional-settings.custom=val1"
        ));
        assertThat(configs, equalTo(Collections.singletonList(
                createConfig("foo", "testQueue",
                        QueueSettings.builder()
                                .withBetweenTaskTimeout(Duration.ofMillis(100L))
                                .withNoTaskTimeout(Duration.ofSeconds(5L))
                                .withThreadCount(3)
                                .withFatalCrashTimeout(Duration.ofHours(1))
                                .withRetryType(TaskRetryType.FIXED_INTERVAL)
                                .withProcessingMode(ProcessingMode.USE_EXTERNAL_EXECUTOR)
                                .withAdditionalSettings(new LinkedHashMap<String, String>() {{
                                    put(QueueSettings.AdditionalSetting.RETRY_FIXED_INTERVAL_DELAY.getName(), "PT3H");
                                    put("custom", "val1");
                                }})
                                .build()))));
    }

    @Test
    public void should_not_parse_properties_in_bad_format() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Cannot parse queue settings:" + System.lineSeparator() +
                "unrecognized setting name: setting=q.testQueue*.processing-mode"));
        QueueConfigsReader queueConfigsReader = new QueueConfigsReader("q");
        queueConfigsReader.parse(fileSystem.write(
                "q.testQueue*.processing-mode=unknown-mode"
        ));
    }

    @Test
    public void should_show_parsing_errors_for_full_config() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Cannot parse queue settings:" + System.lineSeparator() +
                "cannot parse setting: name=between-task-timeout, value=between-task" + System.lineSeparator() +
                "cannot parse setting: name=fatal-crash-timeout, value=fatal-crash" + System.lineSeparator() +
                "cannot parse setting: name=no-task-timeout, value=no-task" + System.lineSeparator() +
                "cannot parse setting: name=thread-count, value=count" + System.lineSeparator() +
                "unknown processing mode: name=unknown-mode2" + System.lineSeparator() +
                "unknown retry type: name=unknown-retry-type" + System.lineSeparator() +
                "unknown setting: name=unknown1, value=unknown-val"));
        QueueConfigsReader queueConfigsReader = new QueueConfigsReader("q");
        queueConfigsReader.parse(fileSystem.write(
                "q.testQueue.table=foo",
                "q.testQueue.between-task-timeout=between-task",
                "q.testQueue.no-task-timeout=no-task",
                "q.testQueue.fatal-crash-timeout=fatal-crash",
                "q.testQueue.thread-count=count",
                "q.testQueue.retry-type=unknown-retry-type",
                "q.testQueue.processing-mode=unknown-mode1",
                "q.testQueue.processing-mode=unknown-mode2",
                "q.testQueue.unknown1=unknown-val"
        ));
    }

    @Test
    public void should_read_and_override_simple_config() throws Exception {
        QueueConfigsReader queueConfigsReader = new QueueConfigsReader("q");
        Collection<QueueConfig> configs = queueConfigsReader.parse(fileSystem.write(
                "q.testQueue.table=foo",
                "q.testQueue.between-task-timeout=PT0.1S",
                "q.testQueue.no-task-timeout=PT5S"), fileSystem.write(
                "q.testQueue.table=bar",
                "q.testQueue.thread-count=2",
                "q.testQueue.between-task-timeout=PT1S",
                "q.testQueue.no-task-timeout=PT10S"));
        assertThat(configs, equalTo(Collections.singletonList(
                createConfig("bar", "testQueue",
                        QueueSettings.builder()
                                .withThreadCount(2)
                                .withBetweenTaskTimeout(Duration.ofSeconds(1L))
                                .withNoTaskTimeout(Duration.ofSeconds(10L)).build()))));
    }

    @Test
    public void should_validate_for_required_settings() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("Cannot parse queue settings:" + System.lineSeparator() +
                "between-task-timeout setting is required: queueName=testQueue" + System.lineSeparator() +
                "no-task-timeout setting is required: queueName=testQueue" + System.lineSeparator() +
                "table setting is required: queueName=testQueue"));

        QueueConfigsReader queueConfigsReader = new QueueConfigsReader("q");
        queueConfigsReader.parse(fileSystem.write("q.testQueue.threads=1"));
    }


    @Test
    public void should_check_file_existence() throws Exception {
        QueueConfigsReader queueConfigsReader = new QueueConfigsReader("q");
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("cannot read queue properties: file=invalid");
        queueConfigsReader.parse(fileSystem.getValue().getPath("invalid"));
    }

    @Test
    public void should_parse_retry_types() throws Exception {
        QueueConfigsReader queueConfigsReader = new QueueConfigsReader("q");
        Collection<QueueConfig> configs = queueConfigsReader.parse(fileSystem.write(
                "q.testQueue1.table=foo",
                "q.testQueue1.between-task-timeout=PT0S",
                "q.testQueue1.no-task-timeout=PT0S",
                "q.testQueue1.retry-type=geometric",

                "q.testQueue2.table=foo",
                "q.testQueue2.between-task-timeout=PT0S",
                "q.testQueue2.no-task-timeout=PT0S",
                "q.testQueue2.retry-type=arithmetic",

                "q.testQueue3.table=foo",
                "q.testQueue3.between-task-timeout=PT0S",
                "q.testQueue3.no-task-timeout=PT0S",
                "q.testQueue3.retry-type=fixed-interval"
        ));
        assertThat(configs.stream().collect(Collectors.toMap(
                config -> config.getLocation().getQueueName(),
                config -> config.getSettings().getRetryType())),
                equalTo(new LinkedHashMap<String, TaskRetryType>() {{
                    put("testQueue1", TaskRetryType.GEOMETRIC_BACKOFF);
                    put("testQueue2", TaskRetryType.ARITHMETIC_BACKOFF);
                    put("testQueue3", TaskRetryType.FIXED_INTERVAL);
                }}));
    }

    @Test
    public void should_parse_processing_modes() throws Exception {
        QueueConfigsReader queueConfigsReader = new QueueConfigsReader("q");
        Collection<QueueConfig> configs = queueConfigsReader.parse(fileSystem.write(
                "q.testQueue1.table=foo",
                "q.testQueue1.between-task-timeout=PT0S",
                "q.testQueue1.no-task-timeout=PT0S",
                "q.testQueue1.processing-mode=separate-transactions",

                "q.testQueue2.table=foo",
                "q.testQueue2.between-task-timeout=PT0S",
                "q.testQueue2.no-task-timeout=PT0S",
                "q.testQueue2.processing-mode=wrap-in-transaction",

                "q.testQueue3.table=foo",
                "q.testQueue3.between-task-timeout=PT0S",
                "q.testQueue3.no-task-timeout=PT0S",
                "q.testQueue3.processing-mode=use-external-executor"
        ));
        assertThat(configs.stream().collect(Collectors.toMap(
                config -> config.getLocation().getQueueName(),
                config -> config.getSettings().getProcessingMode())),
                equalTo(new LinkedHashMap<String, ProcessingMode>() {{
                    put("testQueue1", ProcessingMode.SEPARATE_TRANSACTIONS);
                    put("testQueue2", ProcessingMode.WRAP_IN_TRANSACTION);
                    put("testQueue3", ProcessingMode.USE_EXTERNAL_EXECUTOR);
                }}));
    }

    final class FileSystemRule implements TestRule {

        private AtomicInteger counter = new AtomicInteger();

        private FileSystem fileSystem;

        Path write(String... lines) throws IOException {
            Path path = FileSystemRule.this.fileSystem.getPath(Integer.toString(counter.incrementAndGet()));
            Files.write(path, Arrays.asList(lines), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
            return path;
        }

        private FileSystem getValue() {
            return fileSystem;
        }

        @Override
        public Statement apply(final Statement base, Description description) {
            return new Statement() {

                @Override
                public void evaluate() throws Throwable {
                    try {
                        fileSystem = MemoryFileSystemBuilder.newEmpty().build("inmemory");
                        base.evaluate();
                    } finally {
                        fileSystem.close();
                    }
                }

            };
        }

    }

    @Rule
    public FileSystemRule fileSystem = new FileSystemRule();

}