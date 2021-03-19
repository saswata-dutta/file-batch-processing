package org.saswata.filebatchprocessing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class FileBatchProcessor {
    private static final Logger logger = LogManager.getLogger(FileBatchProcessor.class);

    private final String inputLocation;
    private final int chunkSize;
    private final int parallelism;
    private final int retryCount;
    private final int retryDelaySecs;
    private final Consumer<String[]> action;

    private int lineCount;
    private final ArrayList<String> buffer;
    private final ExecutorService executorService;


    public FileBatchProcessor(String inputLocation,
                              int chunkSize, int parallelism,
                              Consumer<String[]> action,
                              int retryCount, int retryDelaySecs) {

        this.inputLocation = inputLocation;
        this.chunkSize = chunkSize;
        this.parallelism = parallelism;
        this.action = action;
        this.retryCount = retryCount;
        this.retryDelaySecs = retryDelaySecs;

        validateParams();

        lineCount = 0;
        buffer = new ArrayList<>(chunkSize);
        executorService =
                // similar params as delegated inside newFixedThreadPool
                new ThreadPoolExecutor(
                        parallelism,
                        parallelism,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new ExecutorBlockingQueue<>(parallelism));
    }

    private void validateParams() {
        if (chunkSize < 1 || parallelism < 1 || retryCount < 1 || retryDelaySecs < 0 || action == null)
            throw new IllegalArgumentException("Provide sensible values for params");
    }

    public void run() {

        try (Stream<String> lines = Files.lines(Paths.get(inputLocation))) {

            lines.forEach(line -> accumulateLine(line.trim()));
            if (!buffer.isEmpty()) {
                queueChunk();
            }

            shutdownAndAwaitTermination();
        } catch (Exception e) {
            logger.error("Error processing file : " + inputLocation, e);
        }
    }

    private void accumulateLine(String line) {
        ++lineCount;

        if (line.length() > 0) {
            buffer.add(line);
            if (buffer.size() >= chunkSize) {
                queueChunk();
            }
        }
    }

    private void queueChunk() {
        String[] chunk = buffer.toArray(new String[0]);
        String chunkId = String.valueOf(lineCount);
        buffer.clear();

        logger.info("Queued Chunk ending at line : {}", chunkId);

        executorService.submit(() -> processChunk(chunk, chunkId));
    }

    private void processChunk(String[] chunk, String chunkId) {
        for (int i = 0; i < retryCount; ++i) {
            try {
                logger.info("Chunk {} Attempt {}", chunkId, i);
                action.accept(chunk);
                return;
            } catch (Exception e) {
                logger.error("Chunk {} Error in Processing", chunkId, e);
                if (i < retryCount - 1) {
                    logger.info("Chunk {} Pausing {}s before retry", chunkId, retryDelaySecs);
                    pauseBetweenRetry();
                }
            }
        }
        logger.error("Chunk {} Exhausted retries", chunkId);
    }

    private void pauseBetweenRetry() {
        try {
            TimeUnit.SECONDS.sleep(retryDelaySecs);
        } catch (InterruptedException e) {
            logger.error("Interrupted Thread in sleep", e);
            Thread.currentThread().interrupt();
        }
    }

    private void shutdownAndAwaitTermination() {
        executorService.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!executorService.awaitTermination(30, TimeUnit.MINUTES)) {
                executorService.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executorService.awaitTermination(5, TimeUnit.MINUTES))
                    logger.error("Pool did not terminate");
            }
        } catch (InterruptedException e) {
            logger.error("Pool shutdown interrupted", e);
            // (Re-)Cancel if current thread also interrupted
            executorService.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}
