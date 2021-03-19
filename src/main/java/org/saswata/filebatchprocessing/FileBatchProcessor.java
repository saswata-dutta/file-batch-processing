package org.saswata.filebatchprocessing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class FileBatchProcessor {
    private static final Logger logger = LogManager.getLogger(FileBatchProcessor.class);

    private final String inputLocation;
    private final int chunkSize;
    private final int retryCount;
    private final int retryDelaySecs;
    private final ArrayList<String> buffer;
    private final Consumer<String[]> action;
    private int lineCount = 0;

    public FileBatchProcessor(String inputLocation, int chunkSize,
                              Consumer<String[]> action,
                              int retryCount, int retryDelaySecs) {
        this.inputLocation = inputLocation;
        this.chunkSize = chunkSize;
        buffer = new ArrayList<>(chunkSize);
        this.action = action;

        this.retryCount = retryCount;
        this.retryDelaySecs = retryDelaySecs;
    }

    public void run() {

        try (Stream<String> lines = Files.lines(Paths.get(inputLocation))) {

            lines.forEach(line -> accumulateLine(line.trim()));
            if (buffer.size() > 0) {
                queueChunk();
            }
        } catch (Exception e) {
            logger.error("Error processing file : " + inputLocation, e);
        }
    }

    private void accumulateLine(String line) {
        ++lineCount;

        if (line.length() > 0) {
            buffer.add(line);
            if (buffer.size() > chunkSize) {
                queueChunk();
            }
        }
    }

    private void queueChunk() {
        String[] chunk = buffer.toArray(new String[0]);
        buffer.clear();

        logger.info("Queued Chunk ending at line : " + lineCount);
        processChunk(chunk);
    }

    private void processChunk(String[] chunk) {
        for (int i = 0; i < retryCount; ++i) {
            try {
                System.out.println(Thread.currentThread().getName() + " : Attempt : " + i);
                action.accept(chunk);
                return;
            } catch (Exception e) {
                logger.error("Failed Processing Chunk", e);
                if (i < retryCount - 1) {
                    pauseBetweenRetry();
                }
            }
        }
        logger.error("Exhausted retries");
    }

    private void pauseBetweenRetry() {
        try {
            logger.info("Pause : " + retryDelaySecs + "s");
            TimeUnit.SECONDS.sleep(retryDelaySecs);
        } catch (InterruptedException e) {
            logger.error("Interrupted Thread in sleep", e);
            Thread.currentThread().interrupt();
        }
    }
}
