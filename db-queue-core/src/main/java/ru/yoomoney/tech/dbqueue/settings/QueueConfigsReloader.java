package ru.yoomoney.tech.dbqueue.settings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.config.QueueService;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Dynamic reload of queue configuration.
 * <p>
 * Reloads queue configuration if source files has been changed.
 *
 * @author Oleg Kandaurov
 * @since 12.10.2021
 */
public class QueueConfigsReloader {

    private static final Logger log = LoggerFactory.getLogger(QueueConfigsReloader.class);

    @Nonnull
    private final QueueConfigsReader queueConfigsReader;
    @Nonnull
    private final QueueService queueService;
    @Nonnull
    private final List<FileWatcher> fileWatchers;

    /**
     * Constructor
     *
     * @param queueConfigsReader queue configuration parser
     * @param queueService       queue service
     */
    public QueueConfigsReloader(@Nonnull QueueConfigsReader queueConfigsReader,
                                @Nonnull QueueService queueService) {
        this.queueConfigsReader = Objects.requireNonNull(queueConfigsReader, "queueConfigsReader");
        this.queueService = Objects.requireNonNull(queueService, "queueService");
        this.fileWatchers = queueConfigsReader.getConfigPaths().stream()
                .map(path -> new FileWatcher(path, this::reload))
                .collect(Collectors.toList());
    }

    private synchronized void reload() {
        try {
            List<QueueConfig> queueConfigs = queueConfigsReader.parse();
            Map<QueueId, String> diff = queueService.updateQueueConfigs(queueConfigs);
            log.info("queue configuration updated: diff={}", diff);
        } catch (RuntimeException exc) {
            log.error("cannot reload queue configs", exc);
        }
    }

    /**
     * Starts automatic reload of queue configuration
     */
    public synchronized void start() {
        fileWatchers.forEach(FileWatcher::startWatch);
    }

    /**
     * Stops automatic reload of queue configuration
     */
    public synchronized void stop() {
        fileWatchers.forEach(FileWatcher::stopWatch);
    }
}
