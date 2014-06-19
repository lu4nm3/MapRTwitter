package com.attensity.mapr;

import com.attensity.core.StreamManager;
import com.attensity.twitter.ClientFactory;
import com.attensity.twitter.TwitterExtractor;
import com.attensity.twitter.TwitterStreamManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author lmedina
 */
public class MapR {
    private Config configuration;

    private BlockingQueue<String> messageQueue;
    private StreamManager streamManager;

    private ExecutorService executorService;
    private List<TwitterExtractor> extractors;

    private static MapR mapR = new MapR();

    public static void main(String[] args) {
        mapR.init();
        mapR.start();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        mapR.stop();

        System.exit(0);
    }

    private void init() {
        configuration = ConfigFactory.load("twitter/application.conf");
        messageQueue = new LinkedBlockingQueue<>();
        extractors = new LinkedList<>();
        streamManager = new TwitterStreamManager(new ClientFactory(configuration, messageQueue).create());
    }

    private void start() {
        streamManager.connect();
        processMessages();
    }

    private void stop() {
        streamManager.disconnect();
        shutdownProcessingLoop();
    }

    private void processMessages() {
        executorService = Executors.newFixedThreadPool(5);

        for (int i = 0; i < 5; i++) {
            TwitterExtractor extractor = new TwitterExtractor(messageQueue);
            extractors.add(extractor);

            executorService.submit(extractor);
        }
    }

    private void shutdownProcessingLoop() {
//        if (null != executorService) {
//            executorService.shutdownNow();
//        }
        for (TwitterExtractor extractor : extractors) {
            extractor.shutdown();
        }
    }
}
