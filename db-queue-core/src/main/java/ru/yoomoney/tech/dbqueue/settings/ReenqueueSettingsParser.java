package ru.yoomoney.tech.dbqueue.settings;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_REENQUEUE_RETRY_DELAY;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_REENQUEUE_RETRY_INITIAL_DELAY;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_REENQUEUE_RETRY_PLAN;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_REENQUEUE_RETRY_RATIO;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_REENQUEUE_RETRY_STEP;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_REENQUEUE_RETRY_TYPE;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.VALUE_REENQUEUE_RETRY_TYPE_ARITHMETIC;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.VALUE_REENQUEUE_RETRY_TYPE_FIXED;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.VALUE_REENQUEUE_RETRY_TYPE_GEOMETRIC;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.VALUE_REENQUEUE_RETRY_TYPE_MANUAL;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.VALUE_REENQUEUE_RETRY_TYPE_SEQUENTIAL;

public class ReenqueueSettingsParser {

    private final Supplier<ReenqueueSettings.Builder> defaultSettings;
    private final List<String> errorMessages;

    ReenqueueSettingsParser(Supplier<ReenqueueSettings.Builder> defaultSettings, List<String> errorMessages) {
        this.defaultSettings = defaultSettings;
        this.errorMessages = errorMessages;
    }

    Optional<ReenqueueSettings> parseSettings(String queueId, Map<String, String> settings) {
        try {
            ReenqueueSettings.Builder reenqueueSettings = defaultSettings.get();
            settings.forEach((key, value) -> fillSettings(reenqueueSettings, key, value));
            return Optional.of(reenqueueSettings.build());
        } catch (RuntimeException exc) {
            errorMessages.add(String.format("cannot build reenqueue settings: queueId=%s, msg=%s", queueId, exc.getMessage()));
            return Optional.empty();
        }
    }

    private void fillSettings(ReenqueueSettings.Builder builder, String name, String value) {
        try {
            switch (name) {
                case SETTING_REENQUEUE_RETRY_TYPE:
                    builder.withRetryType(parseReenqueueRetryType(value));
                    return;
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
                    return;
            }
        } catch (RuntimeException exc) {
            errorMessages.add(String.format("cannot parse setting: name=%s, value=%s, exception=%s", name, value,
                    exc.getClass().getSimpleName() + '(' + exc.getMessage() + ')'));
        }
    }

    private ReenqueueRetryType parseReenqueueRetryType(String type) {
        switch (type) {
            case VALUE_REENQUEUE_RETRY_TYPE_MANUAL:
                return ReenqueueRetryType.MANUAL;
            case VALUE_REENQUEUE_RETRY_TYPE_FIXED:
                return ReenqueueRetryType.FIXED;
            case VALUE_REENQUEUE_RETRY_TYPE_SEQUENTIAL:
                return ReenqueueRetryType.SEQUENTIAL;
            case VALUE_REENQUEUE_RETRY_TYPE_ARITHMETIC:
                return ReenqueueRetryType.ARITHMETIC;
            case VALUE_REENQUEUE_RETRY_TYPE_GEOMETRIC:
                return ReenqueueRetryType.GEOMETRIC;
            default:
                throw new IllegalArgumentException(String.format("unknown reenqueue retry type: type=%s", type));
        }
    }

    private List<Duration> parseReenqueueRetryPlan(String plan) {
        String[] values = plan.split(",");
        return Arrays.stream(values)
                .map(Duration::parse)
                .collect(Collectors.toList());
    }
}
