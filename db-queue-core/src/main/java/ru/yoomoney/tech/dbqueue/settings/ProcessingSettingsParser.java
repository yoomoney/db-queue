package ru.yoomoney.tech.dbqueue.settings;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_PROCESSING_MODE;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_THREAD_COUNT;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.VALUE_PROCESSING_MODE_SEPARATE_TRANSACTIONS;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.VALUE_PROCESSING_MODE_USE_EXTERNAL_EXECUTOR;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.VALUE_PROCESSING_MODE_WRAP_IN_TRANSACTION;

public class ProcessingSettingsParser {

    private final Supplier<ProcessingSettings.Builder> defaultSettings;
    private final List<String> errorMessages;

    ProcessingSettingsParser(Supplier<ProcessingSettings.Builder> defaultSettings, List<String> errorMessages) {
        this.defaultSettings = defaultSettings;
        this.errorMessages = errorMessages;
    }

    Optional<ProcessingSettings> parseSettings(String queueId, Map<String, String> settings) {
        try {
            ProcessingSettings.Builder processingSettings = defaultSettings.get();
            settings.forEach((key, value) -> fillSettings(processingSettings, key, value));
            return Optional.of(processingSettings.build());
        } catch (RuntimeException exc) {
            errorMessages.add(String.format("cannot build processing settings: queueId=%s, msg=%s", queueId, exc.getMessage()));
            return Optional.empty();
        }
    }

    private void fillSettings(ProcessingSettings.Builder processingSettings, String name, String value) {
        try {
            switch (name) {
                case SETTING_THREAD_COUNT:
                    processingSettings.withThreadCount(Integer.valueOf(value));
                    return;
                case SETTING_PROCESSING_MODE:
                    processingSettings.withProcessingMode(parseProcessingMode(value));
                    return;
                default:
                    return;

            }
        } catch (RuntimeException exc) {
            errorMessages.add(String.format("cannot parse setting: name=%s, value=%s, exception=%s", name, value,
                    exc.getClass().getSimpleName() + '(' + exc.getMessage() + ')'));
        }
    }

    private ProcessingMode parseProcessingMode(String name) {
        switch (name) {
            case VALUE_PROCESSING_MODE_SEPARATE_TRANSACTIONS:
                return ProcessingMode.SEPARATE_TRANSACTIONS;
            case VALUE_PROCESSING_MODE_WRAP_IN_TRANSACTION:
                return ProcessingMode.WRAP_IN_TRANSACTION;
            case VALUE_PROCESSING_MODE_USE_EXTERNAL_EXECUTOR:
                return ProcessingMode.USE_EXTERNAL_EXECUTOR;
            default:
                throw new IllegalArgumentException(String.format("unknown processing mode: name=%s", name));
        }
    }


}
