package ru.yoomoney.tech.dbqueue.settings;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_BETWEEN_TASK_TIMEOUT;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_FATAL_CRASH_TIMEOUT;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_NO_TASK_TIMEOUT;

public class PollSettingsParser {

    private final Supplier<PollSettings.Builder> defaultSettings;
    private final List<String> errorMessages;

    PollSettingsParser(Supplier<PollSettings.Builder> defaultSettings, List<String> errorMessages) {
        this.defaultSettings = defaultSettings;
        this.errorMessages = errorMessages;
    }

    Optional<PollSettings> parseSettings(String queueId, Map<String, String> settings) {
        try {
            PollSettings.Builder pollSettings = defaultSettings.get();
            settings.forEach((key, value) -> fillSettings(pollSettings, key, value));
            return Optional.of(pollSettings.build());
        } catch (RuntimeException exc) {
            errorMessages.add(String.format("cannot build poll settings: queueId=%s, msg=%s", queueId, exc.getMessage()));
            return Optional.empty();
        }
    }

    private void fillSettings(PollSettings.Builder pollSettings, String name, String value) {
        try {
            switch (name) {
                case SETTING_NO_TASK_TIMEOUT:
                    pollSettings.withNoTaskTimeout(Duration.parse(value));
                    return;
                case SETTING_BETWEEN_TASK_TIMEOUT:
                    pollSettings.withBetweenTaskTimeout(Duration.parse(value));
                    return;
                case SETTING_FATAL_CRASH_TIMEOUT:
                    pollSettings.withFatalCrashTimeout(Duration.parse(value));
                    return;
                default:
                    return;

            }
        } catch (RuntimeException exc) {
            errorMessages.add(String.format("cannot parse setting: name=%s, value=%s, exception=%s", name, value,
                    exc.getClass().getSimpleName() + '(' + exc.getMessage() + ')'));
        }
    }

}
