package ru.yoomoney.tech.dbqueue.settings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Provides tracking changes in a target file
 *
 * @author Oleg Kandaurov
 * @since 12.10.2021
 */
@ThreadSafe
class FileWatcher {

    private static final Logger log = LoggerFactory.getLogger(FileWatcher.class);

    @Nonnull
    private final ExecutorService executor;
    @Nonnull
    private final Path watchedFile;
    @Nonnull
    private final Path watchedDir;
    @Nonnull
    private final Runnable onChangeCallback;

    private WatchService watchServiceFileDir;

    /**
     * Constructor
     *
     * @param watchedFile      file to watch
     * @param onChangeCallback callback invoked on file change
     */
    FileWatcher(@Nonnull Path watchedFile,
                @Nonnull Runnable onChangeCallback) {
        this.onChangeCallback = Objects.requireNonNull(onChangeCallback, "onChangeCallback must not be null");
        this.executor = Executors.newSingleThreadExecutor();
        this.watchedFile = Objects.requireNonNull(watchedFile, "watchedFile must not be null");
        this.watchedDir = watchedFile.getParent();
        if (watchedDir == null) {
            throw new IllegalArgumentException("directory of watched file is empty");
        }
        if (!watchedFile.toFile().isFile()) {
            throw new IllegalArgumentException("watched file is not a file: file=" + watchedFile);
        }
    }

    /**
     * Start track file changes
     */
    synchronized void startWatch() {
        log.info("Starting watch for file changes: file={}", watchedFile);
        try {
            startWatchFileDirectory();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Stop track file changes
     */
    synchronized void stopWatch() {
        try {
            log.info("Stopping watch for file changes: file={}", watchedFile);
            if (watchServiceFileDir != null) {
                watchServiceFileDir.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void startWatchFileDirectory() throws IOException {
        if (watchServiceFileDir != null) {
            watchServiceFileDir.close();
        }
        watchServiceFileDir = watchedDir.getFileSystem().newWatchService();
        watchedDir.register(
                watchServiceFileDir,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE,
                StandardWatchEventKinds.ENTRY_MODIFY
        );
        executor.execute(() -> doWatch(watchServiceFileDir, watchedFile.toFile(), onChangeCallback));
    }

    private static void doWatch(WatchService watchService, File file, Runnable callback) {
        try {
            WatchKey watchKey;
            while ((watchKey = watchService.take()) != null) {
                List<WatchEvent<?>> polledEvents = watchKey.pollEvents();
                boolean fileModified = polledEvents
                        .stream()
                        .filter(watchEvent -> !Objects.equals(watchEvent.kind(), StandardWatchEventKinds.OVERFLOW))
                        .map(watchEvent -> watchEvent.context().toString())
                        .anyMatch(fileName -> fileName.equals(file.getName()));
                if (fileModified) {
                    callback.run();
                }
                watchKey.reset();
            }
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
        } catch (ClosedWatchServiceException ignored) {
        }
    }
}
