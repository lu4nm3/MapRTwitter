package com.attensity.mapr;

import com.attensity.WriteMode;
import com.attensity.core.RunnableWriter;
import com.typesafe.config.Config;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lmedina
 */
public class MapRWriter extends RunnableWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapRWriter.class);

    private static final long TIMEOUT = 5;

    private Config configuration;
    private AtomicLong messageCount;
    private BlockingQueue<String> twitterMessageQueue;
    private WriteMode writeMode;

    private AtomicBoolean shutdown = new AtomicBoolean(false);

    // MapR
    private Configuration conf;
    private FileSystem fileSystem;
    private Path dirPath;
    private Path wFilePath;
    private Path rFilePath;
    private FSDataOutputStream outputStream;
//    private FSDataInputStream inputStream;

    public MapRWriter(Config configuration, AtomicLong messageCount, BlockingQueue<String> twitterMessageQueue, WriteMode writeMode) {
        this.configuration = configuration;
        this.messageCount = messageCount;
        this.twitterMessageQueue = twitterMessageQueue;
        this.writeMode = writeMode;

        if (configuration.getString(com.attensity.configuration.Configuration.WRITE_MODE).contains("mapR")) {
            initMapR();
        }
    }

    private void initMapR() {
        try {
            String dirName = configuration.getString(com.attensity.configuration.Configuration.MapR.Raw.DIR_NAME);
            conf = new Configuration();
            fileSystem = FileSystem.get(conf);
            dirPath = new Path(dirName + "/dir");
            wFilePath = new Path(dirName + "/file.w");
            //rFilePath = wFilePath;//new Path(DIR_NAME + "file.r");

            outputStream = fileSystem.create(wFilePath,
                                             true,
                                             512,
                                             (short) 1,
                                             (long)(64*1024*1024));

            LOGGER.info("wFilePath - " + wFilePath);
//            inputStream = fileSystem.open(rFilePath);
        } catch (IOException e) {
            LOGGER.error("Unable to initialize MapR output stream.", e);
        } catch (Exception e) {
            LOGGER.error("There was a problem initializing variables for MapR.", e);
        }
    }

    @Override
    public void run() {
        while (shouldContinue()) {
            try {
                String json = twitterMessageQueue.poll(TIMEOUT, TimeUnit.MILLISECONDS);

                if (StringUtils.isNotBlank(json)) {
                    switch (writeMode) {
                        case MAPR_RAW_UNCOMPRESSED: {
                            writeRawUncompressedToMapR(json);
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

        if (null != outputStream) {
            try {
                outputStream.close();
                LOGGER.info("Finished writing to MapR.");
            } catch (IOException e) {
                LOGGER.error("Unable to close MapR output stream.", e);
            } catch (Exception e) {
                LOGGER.error("Unknown error closing the MapR output stream.", e);
            }
        }
    }

    private boolean shouldContinue() {
        return !shutdown.get();
    }

    private void writeRawUncompressedToMapR(String json) {
//        LOGGER.info("Twitter Message - " + json);

        System.out.println(json);
        byte[] messageBytes = json.getBytes();

        try {
            outputStream.write(messageBytes);
            messageCount.incrementAndGet();
        } catch (IOException e) {
            LOGGER.error("Error writing to the MapR output stream.");
        } catch (Exception e) {
            LOGGER.error("Unknown error writing to the MapR output stream.", e);
        }
    }

//    private ObjectMapper mapper;
//
//    private Map<String, Object> createMessageMap(String json) {
//        Map<String, Object> messageMap;
//
//        try {
//            messageMap = mapper.readValue(json.getBytes(), new TypeReference<Map<String, Object>>() {});
//            messageMap.put("source", "twitter");
//        } catch (IOException e) {
//            LOGGER.error("ERROR", e);
//            return null;
//        }
//
//        return messageMap;
//    }
}