package ru.yoomoney.tech.dbqueue.settings;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_ID_SEQUENCE;
import static ru.yoomoney.tech.dbqueue.settings.QueueConfigsReader.SETTING_TABLE;

public class QueueLocationParser {

    private final List<String> errorMessages;

    QueueLocationParser(List<String> errorMessages) {
        this.errorMessages = errorMessages;
    }

    Optional<QueueLocation> parseQueueLocation(String queueId, Map<String, String> settings) {
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
