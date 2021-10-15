package ru.yoomoney.tech.dbqueue.settings;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_ID_SEQUENCE;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_TABLE;

/**
 * Parser for {@link QueueLocation}
 *
 * @author Oleg Kandaurov
 * @since 01.10.2021
 */
class QueueLocationParser {

    private final List<String> errorMessages;

    /**
     * Constructor
     *
     * @param errorMessages list of error messages
     */
    QueueLocationParser(@Nonnull List<String> errorMessages) {
        this.errorMessages = Objects.requireNonNull(errorMessages, "errorMessages");
    }

    /**
     * Parse settings
     *
     * @param queueId  raw queue identifier
     * @param settings raw settings
     * @return settings or empty object in case of failure
     */
    Optional<QueueLocation> parseQueueLocation(@Nonnull String queueId, @Nonnull Map<String, String> settings) {
        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(settings, "settings");
        try {
            QueueLocation.Builder queueLocation = QueueLocation.builder();
            queueLocation.withQueueId(new QueueId(queueId));
            settings.forEach((key, value) -> fillSettings(queueLocation, key, value));
            return Optional.of(queueLocation.build());
        } catch (RuntimeException exc) {
            errorMessages.add(String.format("cannot build queue location: queueId=%s, msg=%s", queueId, exc.getMessage()));
            return Optional.empty();
        }
    }

    private void fillSettings(QueueLocation.Builder queueLocation, String name, String value) {
        try {
            switch (name) {
                case SETTING_TABLE:
                    queueLocation.withTableName(value);
                    return;
                case SETTING_ID_SEQUENCE:
                    queueLocation.withIdSequence(value);
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
