package com.attensity;

import com.attensity.core.StreamManager;
import com.attensity.mapr.MapRWriter;
import com.attensity.twitter.ClientFactory;
import com.attensity.twitter.TwitterStreamManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

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
public class Main {
    private Config configuration;

    private BlockingQueue<String> messageQueue;
    private StreamManager streamManager;

    private ExecutorService executorService;
    private List<MapRWriter> extractors;

    private AtomicLong messages = new AtomicLong(0);
    private static final long MAX_MESSAGES = 10000;

    private static Main main = new Main();

    public static void main(String[] args) {
        main.init();
        main.start();

        main.stop();

        System.exit(0);
    }

    private void init() {
        configuration = ConfigFactory.parseFile(new File("configuration/twitter/application.conf"));
        messageQueue = new LinkedBlockingQueue<>();
        extractors = new LinkedList<>();
        streamManager = new TwitterStreamManager(new ClientFactory(configuration, messageQueue).create());
    }

    private void start() {
        streamManager.connect();
        processMessages();


        while (messages.get() < MAX_MESSAGES) {
        }
    }

    private void stop() {
        streamManager.disconnect();
        shutdownProcessingLoop();
    }

    private void processMessages() {
        executorService = Executors.newFixedThreadPool(5);

        for (int i = 0; i < 5; i++) {
            MapRWriter extractor = new MapRWriter(messages, messageQueue, WriteTo.MAPR_RAW_UNCOMPRESSED);
            extractors.add(extractor);

            executorService.submit(extractor);
        }
    }

    private void shutdownProcessingLoop() {
        for (MapRWriter extractor : extractors) {
            extractor.shutdown();
        }
    }
}
