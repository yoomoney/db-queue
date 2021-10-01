package ru.yoomoney.tech.dbqueue.stub;

import ru.yoomoney.tech.dbqueue.settings.FailRetryType;
import ru.yoomoney.tech.dbqueue.settings.FailureSettings;
import ru.yoomoney.tech.dbqueue.settings.PollSettings;
import ru.yoomoney.tech.dbqueue.settings.ProcessingMode;
import ru.yoomoney.tech.dbqueue.settings.ProcessingSettings;
import ru.yoomoney.tech.dbqueue.settings.QueueSettings;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueRetryType;
import ru.yoomoney.tech.dbqueue.settings.ReenqueueSettings;

import java.time.Duration;

public class TestFixtures {

    public static QueueSettings.Builder createQueueSettings() {
        return QueueSettings.builder()
                .withProcessingSettings(createProcessingSettings().build())
                .withPollSettings(createPollSettings().build())
                .withFailureSettings(createFailureSettings().build())
                .withReenqueueSettings(createReenqueueSettings().build());
    }

    public static ProcessingSettings.Builder createProcessingSettings() {
        return ProcessingSettings.builder()
                .withProcessingMode(ProcessingMode.SEPARATE_TRANSACTIONS)
                .withThreadCount(1);
    }

    public static PollSettings.Builder createPollSettings() {
        return PollSettings.builder()
                .withBetweenTaskTimeout(Duration.ofMillis(0))
                .withNoTaskTimeout(Duration.ofMillis(0))
                .withFatalCrashTimeout(Duration.ofSeconds(0));
    }

    public static FailureSettings.Builder createFailureSettings() {
        return FailureSettings.builder()
                .withRetryType(FailRetryType.GEOMETRIC_BACKOFF)
                .withRetryInterval(Duration.ofMinutes(1));
    }

    public static ReenqueueSettings.Builder createReenqueueSettings() {
        return ReenqueueSettings.builder()
                .withRetryType(ReenqueueRetryType.MANUAL);
    }
}
