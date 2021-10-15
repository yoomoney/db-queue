package ru.yoomoney.tech.dbqueue.settings;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
 * # see {@link QueueSettings#getExtSettings()}
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
 * @see FailRetryType
 * @see ProcessingMode
 * @see QueueConfig
 * @since 22.08.2017
 */
@ThreadSafe
public class QueueConfigsReader {

    private static final Logger log = LoggerFactory.getLogger(QueueConfigsReader.class);

    /**
     * Representation of {@link FailRetryType#GEOMETRIC_BACKOFF}
     */
    public static final String VALUE_TASK_RETRY_TYPE_GEOMETRIC = "geometric";
    /**
     * Representation of {@link FailRetryType#ARITHMETIC_BACKOFF}
     */
    public static final String VALUE_TASK_RETRY_TYPE_ARITHMETIC = "arithmetic";
    /**
     * Representation of {@link FailRetryType#LINEAR_BACKOFF}
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
     * Representation of {@link ProcessingSettings#getProcessingMode()}
     */
    public static final String SETTING_PROCESSING_MODE = "processing-mode";
    /**
     * Representation of {@link FailureSettings#getRetryType()}
     */
    public static final String SETTING_RETRY_TYPE = "retry-type";
    /**
     * Representation of {@link FailureSettings#getRetryInterval()}
     */
    public static final String SETTING_RETRY_INTERVAL = "retry-interval";
    /**
     * Representation of {@link ReenqueueSettings#getRetryType()}
     */
    public static final String SETTING_REENQUEUE_RETRY_TYPE = "reenqueue-retry-type";
    /**
     * Representation of {@link ReenqueueSettings#getSequentialPlanOrThrow()}
     */
    public static final String SETTING_REENQUEUE_RETRY_PLAN = "reenqueue-retry-plan";
    /**
     * Representation of {@link ReenqueueSettings#getFixedDelayOrThrow()}
     */
    public static final String SETTING_REENQUEUE_RETRY_DELAY = "reenqueue-retry-delay";
    /**
     * Representation of {@link ReenqueueSettings#getInitialDelayOrThrow()}
     */
    public static final String SETTING_REENQUEUE_RETRY_INITIAL_DELAY = "reenqueue-retry-initial-delay";
    /**
     * Representation of {@link ReenqueueSettings#getArithmeticStepOrThrow()}
     */
    public static final String SETTING_REENQUEUE_RETRY_STEP = "reenqueue-retry-step";
    /**
     * Representation of {@link ReenqueueSettings#getGeometricRatioOrThrow()}
     */
    public static final String SETTING_REENQUEUE_RETRY_RATIO = "reenqueue-retry-ratio";
    /**
     * Representation of {@link ProcessingSettings#getThreadCount()}
     */
    public static final String SETTING_THREAD_COUNT = "thread-count";
    /**
     * Representation of {@link PollSettings#getFatalCrashTimeout()}
     */
    public static final String SETTING_FATAL_CRASH_TIMEOUT = "fatal-crash-timeout";
    /**
     * Representation of {@link PollSettings#getBetweenTaskTimeout()}
     */
    public static final String SETTING_BETWEEN_TASK_TIMEOUT = "between-task-timeout";
    /**
     * Representation of {@link PollSettings#getNoTaskTimeout()}
     */
    public static final String SETTING_NO_TASK_TIMEOUT = "no-task-timeout";
    /**
     * Representation of {@link QueueLocation#getTableName()}
     */
    public static final String SETTING_TABLE = "table";
    /**
     * Representation of {@link QueueLocation#getIdSequence()}
     */
    public static final String SETTING_ID_SEQUENCE = "id-sequence";
    /**
     * Representation of {@link QueueSettings#getExtSettings()}
     */
    public static final String SETTING_ADDITIONAL = "additional-settings";

    private final List<String> errorMessages = new ArrayList<>();

    private static final Set<String> ALLOWED_SETTINGS = new HashSet<>(Arrays.asList(
            SETTING_PROCESSING_MODE, SETTING_BETWEEN_TASK_TIMEOUT, SETTING_TABLE,
            SETTING_NO_TASK_TIMEOUT, SETTING_ID_SEQUENCE, SETTING_FATAL_CRASH_TIMEOUT,
            SETTING_REENQUEUE_RETRY_DELAY, SETTING_REENQUEUE_RETRY_PLAN, SETTING_REENQUEUE_RETRY_INITIAL_DELAY,
            SETTING_REENQUEUE_RETRY_RATIO, SETTING_REENQUEUE_RETRY_TYPE, SETTING_REENQUEUE_RETRY_STEP,
            SETTING_RETRY_TYPE, SETTING_RETRY_INTERVAL, SETTING_THREAD_COUNT, SETTING_THREAD_COUNT));

    @Nonnull
    private final List<Path> configPaths;
    @Nonnull
    private final String settingsPrefix;
    @Nonnull
    private final Supplier<ProcessingSettings.Builder> defaultProcessingSettings;
    @Nonnull
    private final Supplier<PollSettings.Builder> defaultPollSettings;
    @Nonnull
    private final Supplier<FailureSettings.Builder> defaultFailureSettings;
    @Nonnull
    private final Supplier<ReenqueueSettings.Builder> defaultReenqueueSettings;

    /**
     * Constructor
     *
     * @param configPaths    files to read configuration from.
     * @param settingsPrefix prefix that will be used for queue settings.
     */
    public QueueConfigsReader(@Nonnull List<Path> configPaths, @Nonnull String settingsPrefix) {
        this(configPaths, settingsPrefix, ProcessingSettings::builder, PollSettings::builder,
                FailureSettings::builder, ReenqueueSettings::builder);
    }

    /**
     * Constructor
     *
     * @param configPaths               files to read configuration from.
     * @param settingsPrefix            prefix that will be used for queue settings.
     * @param defaultProcessingSettings default {@link ProcessingSettings}
     * @param defaultPollSettings       default {@link PollSettings}
     * @param defaultFailureSettings    default {@link FailureSettings}
     * @param defaultReenqueueSettings  default {@link ReenqueueSettings}
     */
    public QueueConfigsReader(@Nonnull List<Path> configPaths,
                              @Nonnull String settingsPrefix,
                              @Nonnull Supplier<ProcessingSettings.Builder> defaultProcessingSettings,
                              @Nonnull Supplier<PollSettings.Builder> defaultPollSettings,
                              @Nonnull Supplier<FailureSettings.Builder> defaultFailureSettings,
                              @Nonnull Supplier<ReenqueueSettings.Builder> defaultReenqueueSettings) {
        this.configPaths = Objects.requireNonNull(configPaths);
        this.settingsPrefix = Objects.requireNonNull(settingsPrefix);
        this.defaultProcessingSettings = Objects.requireNonNull(defaultProcessingSettings);
        this.defaultPollSettings = Objects.requireNonNull(defaultPollSettings);
        this.defaultFailureSettings = Objects.requireNonNull(defaultFailureSettings);
        this.defaultReenqueueSettings = Objects.requireNonNull(defaultReenqueueSettings);
        if (configPaths.isEmpty()) {
            throw new IllegalArgumentException("config paths must not be empty");
        }
        List<Path> illegalConfigs = configPaths.stream().filter(path -> !path.toFile().isFile()).collect(Collectors.toList());
        if (!illegalConfigs.isEmpty()) {
            throw new IllegalArgumentException("config path must be a file: files=" + illegalConfigs);
        }
    }

    /**
     * Get paths to queue configs
     *
     * @return paths to queue configs
     */
    @Nonnull
    public List<Path> getConfigPaths() {
        return new ArrayList<>(configPaths);
    }

    /**
     * Try to parse queues configurations.
     *
     * @return parsed queue configurations
     */
    @Nonnull
    public List<QueueConfig> parse() {
        log.info("loading queue configuration: paths={}", configPaths);
        Path configPath = configPaths.get(0);
        Map<String, String> rawSettings = readRawSettings(configPath);
        if (configPaths.size() > 1) {
            List<Path> overrideConfigPaths = configPaths.subList(1, configPaths.size());
            overrideConfigPaths.stream().filter(Objects::nonNull).forEach(path ->
                    overrideExistingSettings(rawSettings, readRawSettings(path)));
        }

        Map<String, Map<String, String>> queues = splitRawSettingsByQueueId(rawSettings);

        QueueLocationParser queueLocationParser = new QueueLocationParser(errorMessages);
        ProcessingSettingsParser processingSettingsParser = new ProcessingSettingsParser(defaultProcessingSettings,
                errorMessages);
        PollSettingsParser pollSettingsParser = new PollSettingsParser(defaultPollSettings,
                errorMessages);
        ReenqueueSettingsParser reenqueueSettingsParser = new ReenqueueSettingsParser(defaultReenqueueSettings,
                errorMessages);
        FailureSettingsParser failureSettingsParser = new FailureSettingsParser(defaultFailureSettings,
                errorMessages);

        List<QueueConfig> queueConfigs = new ArrayList<>();
        queues.forEach((queueId, settings) -> {
            Optional<QueueLocation> queueLocation = queueLocationParser.parseQueueLocation(queueId, settings);

            Optional<ProcessingSettings> processingSettings = processingSettingsParser.parseSettings(queueId, settings);
            Optional<PollSettings> pollSettings = pollSettingsParser.parseSettings(queueId, settings);
            Optional<FailureSettings> failureSettings = failureSettingsParser.parseSettings(queueId, settings);
            Optional<ReenqueueSettings> reenqueueSettings = reenqueueSettingsParser.parseSettings(queueId, settings);

            if (queueLocation.isPresent() && processingSettings.isPresent() && pollSettings.isPresent() &&
                    failureSettings.isPresent() && reenqueueSettings.isPresent()) {
                QueueSettings queueSettings = QueueSettings.builder()
                        .withProcessingSettings(processingSettings.get())
                        .withPollSettings(pollSettings.get())
                        .withFailureSettings(failureSettings.get())
                        .withReenqueueSettings(reenqueueSettings.get())
                        .withExtSettings(parseExtSettings(settings)).build();
                queueConfigs.add(new QueueConfig(queueLocation.get(), queueSettings));
            }
        });
        checkErrors();
        return queueConfigs;
    }


    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    private Map<String, String> readRawSettings(Path filePath) {
        try (InputStream is = Files.newInputStream(filePath)) {
            Properties props = new Properties();
            props.load(is);
            Map<String, String> map = new LinkedHashMap<>();
            for (String name : props.stringPropertyNames()) {
                map.put(name, props.getProperty(name));
            }
            return cleanupProperties(map);
        } catch (IOException ioe) {
            throw new IllegalArgumentException("cannot read queue properties: file=" + filePath, ioe);
        }
    }

    private Map<String, Map<String, String>> splitRawSettingsByQueueId(Map<String, String> rawSettings) {
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        Pattern settingPattern = Pattern.compile(settingsPrefix + "\\.([A-Za-z0-9\\-_]+)\\.(.*)");
        rawSettings.forEach((setting, value) -> {
            Matcher matcher = settingPattern.matcher(setting);
            if (!matcher.matches()) {
                errorMessages.add(String.format("invalid format for setting name: setting=%s", setting));
            } else {
                String queueId = matcher.group(1);
                String settingName = matcher.group(2);
                result.computeIfAbsent(queueId, s -> new LinkedHashMap<>());
                result.get(queueId).put(settingName, value);
            }
        });
        validateSettings(result);
        checkErrors();
        return result;
    }

    @Nonnull
    private static ExtSettings parseExtSettings(Map<String, String> settings) {
        String extSettingsPrefix = SETTING_ADDITIONAL + '.';
        Map<String, String> map = new LinkedHashMap<>();
        for (Map.Entry<String, String> property : settings.entrySet()) {
            if (property.getKey().startsWith(extSettingsPrefix)) {
                map.put(property.getKey().substring(extSettingsPrefix.length()), property.getValue());
            }
        }
        return ExtSettings.builder().withSettings(map).build();
    }


    private void checkErrors() {
        if (!errorMessages.isEmpty()) {
            throw new IllegalArgumentException("Cannot parse queue settings:" + System.lineSeparator() +
                    errorMessages.stream().sorted().collect(Collectors.joining(System.lineSeparator())));
        }
    }

    private static void overrideExistingSettings(Map<String, String> existingSettings,
                                                 Map<String, String> newSettings) {
        newSettings.forEach((key, newValue) -> {
            String existingValue = existingSettings.get(key);
            if (existingValue != null) {
                log.info("overriding queue property: name={}, existingValue={}, newValue={}",
                        key, existingValue, newValue);
            }
            existingSettings.put(key, newValue);
        });
    }

    private Map<String, String> cleanupProperties(Map<String, String> rawProperties) {
        return rawProperties.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(settingsPrefix))
                .filter(entry -> entry.getValue() != null && !entry.getValue().trim().isEmpty())
                .collect(Collectors.toMap(entry -> entry.getKey().trim(), entry -> entry.getValue().trim()));
    }

    private void validateSettings(Map<String, Map<String, String>> queuesSettings) {
        queuesSettings.forEach((queueId, settings) -> {
            for (String setting : settings.keySet()) {
                if (!ALLOWED_SETTINGS.contains(setting) && !setting.startsWith(SETTING_ADDITIONAL + ".")) {
                    errorMessages.add(
                            String.format("%s setting is unknown: queueId=%s", setting, queueId));
                }
            }
        });
    }

}
