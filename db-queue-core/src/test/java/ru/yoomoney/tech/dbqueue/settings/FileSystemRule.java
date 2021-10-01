package ru.yoomoney.tech.dbqueue.settings;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

final class FileSystemRule implements TestRule {

    private final AtomicInteger counter = new AtomicInteger();

    private FileSystem fileSystem;

    Path write(String... lines) throws IOException {
        Path path = fileSystem.getPath(Integer.toString(counter.incrementAndGet()));
        Files.write(path, Arrays.asList(lines), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        return path;
    }

    public FileSystem getValue() {
        return fileSystem;
    }

    @Override
    @Nonnull
    public Statement apply(Statement base, Description description) {
        return new Statement() {

            @Override
            public void evaluate() throws Throwable {
                try {
                    fileSystem = MemoryFileSystemBuilder.newEmpty().build("inmemory");
                    base.evaluate();
                } finally {
                    fileSystem.close();
                }
            }

        };
    }

}
