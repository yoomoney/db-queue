package ru.yoomoney.tech.dbqueue.settings;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_PROCESSING_MODE;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_THREAD_COUNT;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.VALUE_PROCESSING_MODE_SEPARATE_TRANSACTIONS;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.VALUE_PROCESSING_MODE_USE_EXTERNAL_EXECUTOR;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.VALUE_PROCESSING_MODE_WRAP_IN_TRANSACTION;

/**
 * Parser for {@link ProcessingSettings}
 *
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
public class ProcessingSettingsParser {

    private final Supplier<ProcessingSettings.Builder> defaultSettings;
    private final List<String> errorMessages;

    /**
     * Constructor
     *
     * @param defaultSettings default settings
     * @param errorMessages   list of error messages
     */
    ProcessingSettingsParser(@Nonnull Supplier<ProcessingSettings.Builder> defaultSettings,
                             @Nonnull List<String> errorMessages) {
        this.defaultSettings = Objects.requireNonNull(defaultSettings, "defaultSettings");
        this.errorMessages = Objects.requireNonNull(errorMessages, "errorMessages");
    }

    /**
     * Parse settings
     *
     * @param queueId  raw queue identifier
     * @param settings raw settings
     * @return settings or empty object in case of failure
     */
    Optional<ProcessingSettings> parseSettings(@Nonnull String queueId, @Nonnull Map<String, String> settings) {
        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(settings, "settings");
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
            }
        } catch (RuntimeException exc) {
            errorMessages.add(String.format("cannot parse setting: name=%s, value=%s, exception=%s", name, value,
                    exc.getClass().getSimpleName() + '(' + exc.getMessage() + ')'));
        }
    }

    private static ProcessingMode parseProcessingMode(String name) {
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
