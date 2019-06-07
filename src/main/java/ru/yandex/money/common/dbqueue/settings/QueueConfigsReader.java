package ru.yandex.money.common.dbqueue.settings;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Reads queue configuration from file.
 * <p>
 * File should be a regular {@link Properties} file, so format is 'queue-prefix.key.innerkey=val'.
 * Where actual name of 'queue-prefix' is defined in constructor. Settings with other prefixes are ignored.
 * <ul>
 * <li>Key names of queue settings should contain alphanumeric characters, dashes and underscores.</li>
 * <li>Duration settings must be in ISO-8601. see {@link Duration#parse(CharSequence)}</li>
 * </ul>
 * <p>
 * Full configuration looks like:
 * <pre>
 * # see {@link QueueConfigsReader#SETTING_TABLE}
 * queue-prefix.testQueue.table=foo
 *
 * # see {@link QueueConfigsReader#SETTING_BETWEEN_TASK_TIMEOUT}
 * queue-prefix.testQueue.between-task-timeout=PT0.1S
 *
 * # see {@link QueueConfigsReader#SETTING_NO_TASK_TIMEOUT}
 * queue-prefix.testQueue.no-task-timeout=PT1S
 *
 * # see {@link QueueConfigsReader#SETTING_FATAL_CRASH_TIMEOUT}
 * queue-prefix.testQueue.fatal-crash-timeout=PT5S
 *
 * # see {@link QueueConfigsReader#SETTING_THREAD_COUNT}
 * queue-prefix.testQueue.thread-count=3
 *
 * # see {@link QueueConfigsReader#SETTING_RETRY_TYPE}
 * # values are:
 * # {@link QueueConfigsReader#VALUE_TASK_RETRY_TYPE_ARITHMETIC}
 * # {@link QueueConfigsReader#VALUE_TASK_RETRY_TYPE_GEOMETRIC}
 * # {@link QueueConfigsReader#VALUE_TASK_RETRY_TYPE_LINEAR}
 * queue-prefix.testQueue.retry-type=linear
 *
 * # see {@link QueueConfigsReader#SETTING_RETRY_INTERVAL}
 * queue-prefix.testQueue.retry-interval=PT30S
 *
 * # see {@link QueueConfigsReader#SETTING_REENQUEUE_RETRY_TYPE}
 * # values are:
 * # {@link QueueConfigsReader#VALUE_REENQUEUE_RETRY_TYPE_MANUAL}
 * # {@link QueueConfigsReader#VALUE_REENQUEUE_RETRY_TYPE_FIXED}
 * # {@link QueueConfigsReader#VALUE_REENQUEUE_RETRY_TYPE_SEQUENTIAL}
 * # {@link QueueConfigsReader#VALUE_REENQUEUE_RETRY_TYPE_ARITHMETIC}
 * # {@link QueueConfigsReader#VALUE_REENQUEUE_RETRY_TYPE_GEOMETRIC}
 * #
 * # {@link QueueConfigsReader#VALUE_REENQUEUE_RETRY_TYPE_MANUAL} is used by default
 * queue-prefix.testQueue.reenqueue-retry-type=fixed
 *
 * # see {@link QueueConfigsReader#SETTING_REENQUEUE_RETRY_DELAY}
 * # Required when {@link QueueConfigsReader#SETTING_REENQUEUE_RETRY_TYPE} is set to 'fixed'
 * queue-prefix.testQueue.reenqueue-retry-delay=PT10S
 *
 * # see {@link QueueConfigsReader#SETTING_REENQUEUE_RETRY_PLAN}
 * # Required when {@link QueueConfigsReader#SETTING_REENQUEUE_RETRY_TYPE} is set to 'sequential'
 * queue-prefix.testQueue.reenqueue-retry-plan=PT1S,PT2S,PT3S
 *
 * # see {@link QueueConfigsReader#SETTING_REENQUEUE_RETRY_INITIAL_DELAY}
 * # PT1S is used by default.
 * queue-prefix.testQueue.reenqueue-retry-initial-delay=PT10S
 *
 * # see {@link QueueConfigsReader#SETTING_REENQUEUE_RETRY_STEP}
 * # PT2S is used by default.
 * queue-prefix.testQueue.reenqueue-retry-step=PT2S
 *
 * # see {@link QueueConfigsReader#SETTING_REENQUEUE_RETRY_RATIO}
 * # 2 is used by default.
 * queue-prefix.testQueue.reenqueue-retry-ratio=3
 *
 * # see {@link QueueConfigsReader#SETTING_PROCESSING_MODE}
 * # values are:
 * # {@link QueueConfigsReader#VALUE_PROCESSING_MODE_SEPARATE_TRANSACTIONS}
 * # {@link QueueConfigsReader#VALUE_PROCESSING_MODE_USE_EXTERNAL_EXECUTOR}
 * # {@link QueueConfigsReader#VALUE_PROCESSING_MODE_WRAP_IN_TRANSACTION}
 * queue-prefix.testQueue.processing-mode=use-external-executor
 *
 * # see {@link QueueConfigsReader#SETTING_ADDITIONAL}
 * # see {@link QueueSettings#getAdditionalSettings()}
 * queue-prefix.testQueue.additional-settings.custom-val=custom-key
 *
 * # you can define custom settings to use it in enqueueing or processing
 * queue-prefix.testQueue.additional-settings.custom=val1
 * </pre>
 * <p>
 * Where 'testQueue' is name of a queue you apply configuration to.
 *
 * @author Oleg Kandaurov
 * @see QueueSettings
 * @see QueueLocation
 * @see TaskRetryType
 * @see ProcessingMode
 * @see QueueConfig
 * @since 22.08.2017
 */
public class QueueConfigsReader {

    /**
     * Representation of {@link TaskRetryType#GEOMETRIC_BACKOFF}
     */
    public static final String VALUE_TASK_RETRY_TYPE_GEOMETRIC = "geometric";
    /**
     * Representation of {@link TaskRetryType#ARITHMETIC_BACKOFF}
     */
    public static final String VALUE_TASK_RETRY_TYPE_ARITHMETIC = "arithmetic";
    /**
     * Representation of {@link TaskRetryType#LINEAR_BACKOFF}
     */
    public static final String VALUE_TASK_RETRY_TYPE_LINEAR = "linear";
    /**
     * Representation of {@link ReenqueueRetryType#MANUAL}
     */
    public static final String VALUE_REENQUEUE_RETRY_TYPE_MANUAL = "manual";
    /**
     * Representation of {@link ReenqueueRetryType#FIXED}
     */
    public static final String VALUE_REENQUEUE_RETRY_TYPE_FIXED = "fixed";
    /**
     * Representation of {@link ReenqueueRetryType#SEQUENTIAL}
     */
    public static final String VALUE_REENQUEUE_RETRY_TYPE_SEQUENTIAL = "sequential";
    /**
     * Representation of {@link ReenqueueRetryType#ARITHMETIC}
     */
    public static final String VALUE_REENQUEUE_RETRY_TYPE_ARITHMETIC = "arithmetic";
    /**
     * Representation of {@link ReenqueueRetryType#GEOMETRIC}
     */
    public static final String VALUE_REENQUEUE_RETRY_TYPE_GEOMETRIC = "geometric";
    /**
     * Representation of {@link ProcessingMode#USE_EXTERNAL_EXECUTOR}
     */
    public static final String VALUE_PROCESSING_MODE_USE_EXTERNAL_EXECUTOR = "use-external-executor";
    /**
     * Representation of {@link ProcessingMode#WRAP_IN_TRANSACTION}
     */
    public static final String VALUE_PROCESSING_MODE_WRAP_IN_TRANSACTION = "wrap-in-transaction";
    /**
     * Representation of {@link ProcessingMode#SEPARATE_TRANSACTIONS}
     */
    public static final String VALUE_PROCESSING_MODE_SEPARATE_TRANSACTIONS = "separate-transactions";
    /**
     * Representation of {@link QueueSettings#getProcessingMode()}
     */
    public static final String SETTING_PROCESSING_MODE = "processing-mode";
    /**
     * Representation of {@link QueueSettings#getRetryType()}
     */
    public static final String SETTING_RETRY_TYPE = "retry-type";
    /**
     * Representation of {@link QueueSettings#getRetryInterval()}
     */
    public static final String SETTING_RETRY_INTERVAL = "retry-interval";
    private static final String REENQUEUE_RETRY_PREFIX = "reenqueue-retry";
    /**
     * Representation of {@link ReenqueueRetrySettings#getType()}
     */
    public static final String SETTING_REENQUEUE_RETRY_TYPE = REENQUEUE_RETRY_PREFIX + "-type";
    /**
     * Representation of {@link ReenqueueRetrySettings#getSequentialPlanOrThrow()}
     */
    public static final String SETTING_REENQUEUE_RETRY_PLAN = REENQUEUE_RETRY_PREFIX + "-plan";
    /**
     * Representation of {@link ReenqueueRetrySettings#getFixedDelayOrThrow()}
     */
    public static final String SETTING_REENQUEUE_RETRY_DELAY = REENQUEUE_RETRY_PREFIX + "-delay";
    /**
     * Representation of {@link ReenqueueRetrySettings#getInitialDelay()}
     */
    public static final String SETTING_REENQUEUE_RETRY_INITIAL_DELAY = REENQUEUE_RETRY_PREFIX + "-initial-delay";
    /**
     * Representation of {@link ReenqueueRetrySettings#getArithmeticStep()}
     */
    public static final String SETTING_REENQUEUE_RETRY_STEP = REENQUEUE_RETRY_PREFIX + "-step";
    /**
     * Representation of {@link ReenqueueRetrySettings#getGeometricRatio()}
     */
    public static final String SETTING_REENQUEUE_RETRY_RATIO = REENQUEUE_RETRY_PREFIX + "-ratio";
    /**
     * Representation of {@link QueueSettings#getThreadCount()}
     */
    public static final String SETTING_THREAD_COUNT = "thread-count";
    /**
     * Representation of {@link QueueSettings#getFatalCrashTimeout()}
     */
    public static final String SETTING_FATAL_CRASH_TIMEOUT = "fatal-crash-timeout";
    /**
     * Representation of {@link QueueSettings#getBetweenTaskTimeout()}
     */
    public static final String SETTING_BETWEEN_TASK_TIMEOUT = "between-task-timeout";
    /**
     * Representation of {@link QueueSettings#getNoTaskTimeout()}
     */
    public static final String SETTING_NO_TASK_TIMEOUT = "no-task-timeout";
    /**
     * Representation of {@link QueueLocation#getTableName()}
     */
    public static final String SETTING_TABLE = "table";
    /**
     * Representation of {@link QueueSettings#getAdditionalSettings()}
     */
    public static final String SETTING_ADDITIONAL = "additional-settings";
    private static final Logger log = LoggerFactory.getLogger(QueueConfigsReader.class);
    private final List<String> errorMessages = new ArrayList<>();

    @Nonnull
    private final String settingsPrefix;

    /**
     * Constructor
     *
     * @param settingsPrefix prefix that will be used for queue settings.
     */
    public QueueConfigsReader(@Nonnull String settingsPrefix) {
        this.settingsPrefix = Objects.requireNonNull(settingsPrefix);
    }

    /**
     * Try to parse queues configurations.
     *
     * @param configPath         file to read configuration from.
     * @param overrideConfigPath files that override main configuration.
     *                           They can be useful when you work with queues in test environment.
     * @return parsed queue configurations
     */
    @Nonnull
    public Collection<QueueConfig> parse(@Nonnull Path configPath, @Nullable Path... overrideConfigPath) {
        Objects.requireNonNull(configPath, "config path is empty");
        Map<String, String> rawSettings = readRawSettings(configPath);
        if (overrideConfigPath != null) {
            Arrays.stream(overrideConfigPath).filter(Objects::nonNull).forEach(path ->
                    overrideExistingSettings(rawSettings, readRawSettings(path)));
        }

        Map<String, Map<String, String>> queues = splitRawSettingsByQueueId(rawSettings);

        List<QueueLocation> queueLocations = new ArrayList<>();
        List<QueueSettings.Builder> queueSettings = new ArrayList<>();
        queues.forEach((queueId, settings) -> {
            queueLocations.add(buildQueueLocation(queueId, settings));
            queueSettings.add(buildQueueSettings(settings)
                    .withAdditionalSettings(buildAdditionalSettings(settings))
                    .withReenqueueRetrySettings(buildReenqueueRetrySettings(settings)));
        });
        checkErrors();
        return IntStream.range(0, Integer.max(queueLocations.size(), queueSettings.size()))
                .mapToObj(counter -> new QueueConfig(queueLocations.get(counter),
                        queueSettings.get(counter).build()))
                .collect(Collectors.toList());
    }


    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    private Map<String, String> readRawSettings(Path filePath) {
        try (InputStream is = Files.newInputStream(filePath)) {
            Properties props = new Properties();
            props.load(is);
            return cleanupProperties(props.stringPropertyNames().stream()
                    .collect(Collectors.toMap(Function.identity(), props::getProperty)));
        } catch (IOException ioe) {
            throw new IllegalArgumentException("cannot read queue properties: file=" + filePath, ioe);
        }
    }

    private Map<String, Map<String, String>> splitRawSettingsByQueueId(Map<String, String> rawSettings) {
        Map<String, Map<String, String>> result = new HashMap<>();
        Pattern settingPattern = Pattern.compile(settingsPrefix + "\\.([A-Za-z0-9\\-_]+)\\.(.*)");
        rawSettings.forEach((setting, value) -> {
            Matcher matcher = settingPattern.matcher(setting);
            if (!matcher.matches()) {
                errorMessages.add(String.format("unrecognized setting name: setting=%s", setting));
            } else {
                String queueId = matcher.group(1);
                String settingName = matcher.group(2);
                result.computeIfAbsent(queueId, s -> new HashMap<>());
                result.get(queueId).put(settingName, value);
            }
        });
        validateSettings(result);
        checkErrors();
        return result;
    }


    @Nonnull
    private QueueLocation buildQueueLocation(String queueId, Map<String, String> settings) {
        return Objects.requireNonNull(settings.entrySet().stream()
                .filter(property -> SETTING_TABLE.equals(property.getKey()))
                .findFirst()
                .map(property -> QueueLocation.builder().withTableName(property.getValue())
                        .withQueueId(new QueueId(queueId)).build())
                .orElse(null));
    }

    private QueueSettings.Builder buildQueueSettings(Map<String, String> settings) {
        QueueSettings.Builder builder = QueueSettings.builder();
        settings.entrySet().stream()
                .filter(property -> !property.getKey().startsWith(SETTING_ADDITIONAL + "."))
                .filter(property -> !property.getKey().startsWith(REENQUEUE_RETRY_PREFIX))
                .filter(property -> !SETTING_TABLE.equals(property.getKey()))
                .forEach(property -> tryFillSetting(builder, property.getKey(), property.getValue()));
        return builder;
    }

    @Nonnull
    private Map<String, String> buildAdditionalSettings(Map<String, String> settings) {
        String additionalSettingsName = SETTING_ADDITIONAL + ".";
        return settings.entrySet().stream()
                .filter(property -> property.getKey().startsWith(additionalSettingsName))
                .collect(Collectors.toMap(
                        entry -> entry.getKey().substring(additionalSettingsName.length(), entry.getKey().length()),
                        Map.Entry::getValue));
    }

    @Nonnull
    private ReenqueueRetrySettings buildReenqueueRetrySettings(Map<String, String> settings) {
        if (!settings.containsKey(SETTING_REENQUEUE_RETRY_TYPE)) {
            return ReenqueueRetrySettings.createDefault();
        }
        Optional<ReenqueueRetryType> optionalRetryType = parseReenqueueRetryType(settings.get(SETTING_REENQUEUE_RETRY_TYPE));
        if (!optionalRetryType.isPresent()) {
            return ReenqueueRetrySettings.createDefault();
        }
        ReenqueueRetrySettings.Builder builder = ReenqueueRetrySettings.builder(optionalRetryType.get());
        settings.entrySet().stream()
                .filter(property -> property.getKey().startsWith(REENQUEUE_RETRY_PREFIX))
                .filter(property -> !SETTING_REENQUEUE_RETRY_TYPE.equals(property.getKey()))
                .forEach(property -> tryFillReenqueueSetting(builder, property.getKey(), property.getValue()));
        return builder.build();
    }

    private void tryFillReenqueueSetting(ReenqueueRetrySettings.Builder builder, String name, String value) {
        try {
            switch (name) {
                case SETTING_REENQUEUE_RETRY_PLAN:
                    builder.withSequentialPlan(parseReenqueueRetryPlan(value));
                    return;
                case SETTING_REENQUEUE_RETRY_DELAY:
                    builder.withFixedDelay(Duration.parse(value));
                    return;
                case SETTING_REENQUEUE_RETRY_INITIAL_DELAY:
                    builder.withInitialDelay(Duration.parse(value));
                    return;
                case SETTING_REENQUEUE_RETRY_STEP:
                    builder.withArithmeticStep(Duration.parse(value));
                    return;
                case SETTING_REENQUEUE_RETRY_RATIO:
                    builder.withGeometricRatio(Long.valueOf(value));
                    return;
                default:
                    errorMessages.add(String.format("unknown re-enqueue setting: name=%s, value=%s", name, value));
                    return;
            }
        } catch (RuntimeException exc) {
            log.warn("cannot parse setting", exc);
            errorMessages.add(String.format("cannot parse re-enqueue setting: name=%s, value=%s", name, value));
        }
    }

    private void checkErrors() {
        if (!errorMessages.isEmpty()) {
            throw new IllegalArgumentException("Cannot parse queue settings:" + System.lineSeparator() +
                    errorMessages.stream().sorted().collect(Collectors.joining(System.lineSeparator())));
        }
    }

    private void overrideExistingSettings(Map<String, String> existingSettings,
                                          Map<String, String> newSettings) {
        newSettings.forEach((key, value) -> {
            if (existingSettings.containsKey(key)) {
                log.info("overriding queue property: name={}, existingValue={}, newValue={}", key,
                        existingSettings.get(key), value);
            }
            existingSettings.put(key, value);
        });
    }

    private Map<String, String> cleanupProperties(Map<String, String> rawProperties) {
        return rawProperties.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(settingsPrefix))
                .filter(entry -> entry.getValue() != null && !entry.getValue().trim().isEmpty())
                .collect(Collectors.toMap(entry -> entry.getKey().trim(), entry -> entry.getValue().trim()));
    }

    private void validateSettings(Map<String, Map<String, String>> queuesSettings) {
        List<String> requiredSettings = Arrays.asList(SETTING_NO_TASK_TIMEOUT, SETTING_BETWEEN_TASK_TIMEOUT,
                SETTING_TABLE);
        queuesSettings.forEach((queueId, settings) -> requiredSettings.stream()
                .filter(requiredSetting -> !settings.containsKey(requiredSetting))
                .forEach(requiredSetting -> errorMessages.add(
                        String.format("%s setting is required: queueId=%s", requiredSetting, queueId))));
    }

    private void tryFillSetting(QueueSettings.Builder queueSetting, String name, String value) {
        try {
            switch (name) {
                case SETTING_NO_TASK_TIMEOUT:
                    queueSetting.withNoTaskTimeout(Duration.parse(value));
                    return;
                case SETTING_BETWEEN_TASK_TIMEOUT:
                    queueSetting.withBetweenTaskTimeout(Duration.parse(value));
                    return;
                case SETTING_FATAL_CRASH_TIMEOUT:
                    queueSetting.withFatalCrashTimeout(Duration.parse(value));
                    return;
                case SETTING_THREAD_COUNT:
                    queueSetting.withThreadCount(Integer.valueOf(value));
                    return;
                case SETTING_RETRY_TYPE:
                    queueSetting.withRetryType(parseRetryType(value).orElse(null));
                    return;
                case SETTING_RETRY_INTERVAL:
                    queueSetting.withRetryInterval(Duration.parse(value));
                    return;
                case SETTING_PROCESSING_MODE:
                    queueSetting.withProcessingMode(parseProcessingMode(value).orElse(null));
                    return;
                default:
                    errorMessages.add(String.format("unknown setting: name=%s, value=%s", name, value));
                    return;

            }
        } catch (RuntimeException exc) {
            log.warn("cannot parse setting", exc);
            errorMessages.add(String.format("cannot parse setting: name=%s, value=%s", name, value));
        }
    }

    private Optional<ProcessingMode> parseProcessingMode(String name) {
        switch (name) {
            case VALUE_PROCESSING_MODE_SEPARATE_TRANSACTIONS:
                return Optional.of(ProcessingMode.SEPARATE_TRANSACTIONS);
            case VALUE_PROCESSING_MODE_WRAP_IN_TRANSACTION:
                return Optional.of(ProcessingMode.WRAP_IN_TRANSACTION);
            case VALUE_PROCESSING_MODE_USE_EXTERNAL_EXECUTOR:
                return Optional.of(ProcessingMode.USE_EXTERNAL_EXECUTOR);
            default:
                errorMessages.add(String.format("unknown processing mode: name=%s", name));
                return Optional.empty();
        }
    }

    private Optional<TaskRetryType> parseRetryType(String name) {
        switch (name) {
            case VALUE_TASK_RETRY_TYPE_GEOMETRIC:
                return Optional.of(TaskRetryType.GEOMETRIC_BACKOFF);
            case VALUE_TASK_RETRY_TYPE_ARITHMETIC:
                return Optional.of(TaskRetryType.ARITHMETIC_BACKOFF);
            case VALUE_TASK_RETRY_TYPE_LINEAR:
                return Optional.of(TaskRetryType.LINEAR_BACKOFF);
            default:
                errorMessages.add(String.format("unknown retry type: name=%s", name));
                return Optional.empty();
        }
    }

    private Optional<ReenqueueRetryType> parseReenqueueRetryType(String type) {
        switch (type) {
            case VALUE_REENQUEUE_RETRY_TYPE_MANUAL:
                return Optional.of(ReenqueueRetryType.MANUAL);
            case VALUE_REENQUEUE_RETRY_TYPE_FIXED:
                return Optional.of(ReenqueueRetryType.FIXED);
            case VALUE_REENQUEUE_RETRY_TYPE_SEQUENTIAL:
                return Optional.of(ReenqueueRetryType.SEQUENTIAL);
            case VALUE_REENQUEUE_RETRY_TYPE_ARITHMETIC:
                return Optional.of(ReenqueueRetryType.ARITHMETIC);
            case VALUE_REENQUEUE_RETRY_TYPE_GEOMETRIC:
                return Optional.of(ReenqueueRetryType.GEOMETRIC);
            default:
                errorMessages.add(String.format("unknown re-enqueue retry type: type=%s", type));
                return Optional.empty();
        }
    }

    private List<Duration> parseReenqueueRetryPlan(String plan) {
        String[] values = plan.split(",");
        return Arrays.stream(values)
                .map(Duration::parse)
                .collect(Collectors.toList());
    }

}
