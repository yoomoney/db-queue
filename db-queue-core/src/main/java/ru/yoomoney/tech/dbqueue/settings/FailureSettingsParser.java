package ru.yoomoney.tech.dbqueue.settings;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_RETRY_INTERVAL;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_RETRY_TYPE;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.VALUE_TASK_RETRY_TYPE_ARITHMETIC;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.VALUE_TASK_RETRY_TYPE_GEOMETRIC;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.VALUE_TASK_RETRY_TYPE_LINEAR;

public class FailureSettingsParser {

    private final Supplier<FailureSettings.Builder> defaultSettings;
    private final List<String> errorMessages;

    FailureSettingsParser(Supplier<FailureSettings.Builder> defaultSettings, List<String> errorMessages) {
        this.defaultSettings = defaultSettings;
        this.errorMessages = errorMessages;
    }

    Optional<FailureSettings> parseSettings(String queueId, Map<String, String> settings) {
        try {
            FailureSettings.Builder failureSettings = defaultSettings.get();
            settings.forEach((key, value) -> fillSettings(failureSettings, key, value));
            return Optional.of(failureSettings.build());
        } catch (RuntimeException exc) {
            errorMessages.add(String.format("cannot build failure settings: queueId=%s, msg=%s", queueId, exc.getMessage()));
            return Optional.empty();
        }
    }

    private void fillSettings(FailureSettings.Builder failureSettings, String name, String value) {
        try {
            switch (name) {
                case SETTING_RETRY_TYPE:
                    failureSettings.withRetryType(parseRetryType(value));
                    return;
                case SETTING_RETRY_INTERVAL:
                    failureSettings.withRetryInterval(Duration.parse(value));
                    return;
                default:
                    return;

            }
        } catch (RuntimeException exc) {
            errorMessages.add(String.format("cannot parse setting: name=%s, value=%s, exception=%s", name, value,
                    exc.getClass().getSimpleName() + '(' + exc.getMessage() + ')'));
        }
    }

    private FailRetryType parseRetryType(String name) {
        switch (name) {
            case VALUE_TASK_RETRY_TYPE_GEOMETRIC:
                return FailRetryType.GEOMETRIC_BACKOFF;
            case VALUE_TASK_RETRY_TYPE_ARITHMETIC:
                return FailRetryType.ARITHMETIC_BACKOFF;
            case VALUE_TASK_RETRY_TYPE_LINEAR:
                return FailRetryType.LINEAR_BACKOFF;
            default:
                throw new IllegalArgumentException(String.format("unknown retry type: name=%s", name));
        }
    }


}
