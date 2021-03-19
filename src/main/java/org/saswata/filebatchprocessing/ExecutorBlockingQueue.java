package org.saswata.filebatchprocessing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;

public class ExecutorBlockingQueue<T> extends LinkedBlockingQueue<T> {

    private static final Logger logger = LogManager.getLogger(ExecutorBlockingQueue.class);

    public ExecutorBlockingQueue(int size) {
        super(size);
    }

    // ExecutorService calls offer, not put, so "intercept and block" it
    @Override
    public boolean offer(T t) {
        try {
            put(t);
            return true;
        } catch (InterruptedException e) {
            logger.error("Interrupt while waiting to put in queue");
            Thread.currentThread().interrupt();
        }

        return false;
    }
}
