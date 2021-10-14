package ru.yoomoney.tech.dbqueue.settings;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class FileWatcherTest {

    @Test
    public void should_watch_file_changes() throws Exception {
        Path tempDir = Files.createTempDirectory(Paths.get(System.getProperty("user.dir"), "target"),
                getClass().getSimpleName());
        tempDir.toFile().deleteOnExit();

        Path tempFile = Files.createTempFile(tempDir, "queue", ".properties");
        tempFile.toFile().deleteOnExit();

        Files.write(tempFile, "1".getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        AtomicBoolean fileHasChanged = new AtomicBoolean(false);
        FileWatcher fileWatcher = new FileWatcher(tempFile, () -> {
            fileHasChanged.set(true);
        });
        fileWatcher.startWatch();
        // wait for executor start
        Thread.sleep(500);
        Files.write(tempFile, "2".getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        long startTime = System.currentTimeMillis();
        long elapsed = 0;
        while (Duration.ofMillis(elapsed).getSeconds() < 2 && !fileHasChanged.get()) {
            elapsed = System.currentTimeMillis() - startTime;
            Thread.sleep(100);
        }
        fileWatcher.stopWatch();
        assertThat(fileHasChanged.get(), equalTo(true));
    }

}