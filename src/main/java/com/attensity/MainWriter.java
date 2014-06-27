package com.attensity;

import com.attensity.configuration.Configuration;
import com.attensity.core.RunnableWriter;
import com.attensity.core.StreamManager;
import com.attensity.mapr.MapRWriter;
import com.attensity.twitter.ClientFactory;
import com.attensity.twitter.TwitterStreamManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lmedina
 */
public class MainWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MainWriter.class);

    private Config configuration;

    private BlockingQueue<String> messageQueue;
    private StreamManager streamManager;

    private ExecutorService executorService;
    private List<RunnableWriter> writers;

    private AtomicLong messages = new AtomicLong(0);
    private long previousCountValue;

    private static MainWriter mainWriter = new MainWriter();

    public static void main(String[] args) {
        mainWriter.init();
        mainWriter.start();

        mainWriter.stop();

        LOGGER.info("Exiting...");
        System.exit(0);
    }

    private void init() {
        configuration = ConfigFactory.parseFile(new File("configuration/application.conf"));
        messageQueue = new LinkedBlockingQueue<>();
        writers = new LinkedList<>();
        streamManager = new TwitterStreamManager(new ClientFactory(configuration, messageQueue).create());
    }

    private void start() {
        streamManager.connect();
        processMessages();


        int maxMessages = configuration.getInt(Configuration.MAX_MESSAGES);

        while (messages.get() < maxMessages) {
            long currentCount = messages.get();

            if (currentCount % 100 == 0 && currentCount != previousCountValue) {
                LOGGER.info("Message count update - " + currentCount);
                previousCountValue = currentCount;
            }
        }

        LOGGER.info("Final message count - " + messages.get());
    }

    private void stop() {
        streamManager.disconnect();
        shutdownProcessingLoop();
    }

    private void processMessages() {
        createRunnableWriters();

        executorService = Executors.newFixedThreadPool(5);

        for (RunnableWriter writer : writers) {
            executorService.submit(writer);
        }
    }

    private void createRunnableWriters() {
        String mode = configuration.getString(Configuration.WRITE_MODE);
        int numWriters = configuration.getInt(Configuration.NUM_WRITERS);

        LOGGER.info("Mode is " + mode);
        for (int i = 0; i < numWriters; i++) {
            if (mode.equals(WriteMode.MAPR_HIVE_UNCOMPRESSED.getValue())) {
                writers.add(new MapRWriter(configuration, messages, messageQueue, WriteMode.MAPR_HIVE_UNCOMPRESSED));
            }
        }
    }

    private void shutdownProcessingLoop() {
        for (RunnableWriter writer : writers) {
            writer.shutdown();
        }

        LOGGER.info("Shut down writers.");
    }
}
