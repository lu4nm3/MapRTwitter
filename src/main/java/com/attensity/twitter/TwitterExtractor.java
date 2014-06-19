package com.attensity.twitter;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author lmedina
 */
public class TwitterExtractor implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterExtractor.class);

    private static final long TIMEOUT = 5;

    private BlockingQueue<String> messageQueue;
    private ObjectMapper mapper;

    private AtomicBoolean shutdown = new AtomicBoolean(false);

    public TwitterExtractor(BlockingQueue<String> messageQueue) {
        this.messageQueue = messageQueue;
        this.mapper = new ObjectMapper();
    }

    public void shutdown() {
        shutdown.set(true);
    }

    @Override
    public void run() {
        while (shouldContinue()) {
            try {
                String json = messageQueue.poll(TIMEOUT, TimeUnit.MILLISECONDS);

                if (StringUtils.isNotBlank(json)) {
//                    System.out.println(json);
                    Map<String, Object> messageMap = createMessageMap(json);

                    if (null != messageMap) {
                        System.out.println(messageMap);
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.error("error2", e);
            }
        }
    }

    private boolean shouldContinue() {
        return !shutdown.get();
    }

    private Map<String, Object> createMessageMap(String json) {
        Map<String, Object> messageMap;

        try {
            messageMap = mapper.readValue(json.getBytes(), new TypeReference<Map<String, Object>>() {});
            messageMap.put("source", "twitter");
        } catch (IOException e) {
            LOGGER.error("ERROR", e);
            return null;
        }

        return messageMap;
    }

    private void writeToMapR(Map<String, Object> message) {

    }
}