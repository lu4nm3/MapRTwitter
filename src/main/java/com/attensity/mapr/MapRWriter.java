package com.attensity.mapr;

import com.attensity.WriteTo;
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
public class MapRWriter implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapRWriter.class);

    private static final long TIMEOUT = 5;

    private BlockingQueue<String> twitterMessageQueue;
    private WriteTo writeTo;

    private ObjectMapper mapper;

    private AtomicBoolean shutdown = new AtomicBoolean(false);

    public MapRWriter(BlockingQueue<String> twitterMessageQueue, WriteTo writeTo) {
        this.twitterMessageQueue = twitterMessageQueue;
        this.writeTo = writeTo;

        this.mapper = new ObjectMapper();
    }

    public void shutdown() {
        shutdown.set(true);
    }

    @Override
    public void run() {
        while (shouldContinue()) {
            try {
                String json = twitterMessageQueue.poll(TIMEOUT, TimeUnit.MILLISECONDS);

                if (StringUtils.isNotBlank(json)) {
                    switch (writeTo) {
                        case MAPR_RAW_UNCOMPRESSED: {
                            writeToMapRRawUncompressed(json);
                        }
                        case MAPR_RAW_COMPRESSED: {

                        }
                        case MAPR_HIVE_UNCOMPRESSED: {

                        }
                        case MAPR_HIVE_COMPRESSED: {

                        }
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

    private void writeToMapRRawUncompressed(String json) {
        System.out.println(json);
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
}