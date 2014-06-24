package com.attensity.core;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author lmedina
 */
public abstract class RunnableWriter implements Runnable {
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    @Override
    public void run() {

    }

    public void shutdown() {
        shutdown.set(true);
    }
}
