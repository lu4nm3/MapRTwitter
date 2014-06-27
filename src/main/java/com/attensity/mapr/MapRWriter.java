package com.attensity.mapr;

import com.attensity.WriteMode;
import com.attensity.core.RunnableWriter;
import com.mapr.fs.MapRFileSystem;
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
            boolean overwrite = configuration.getBoolean(com.attensity.configuration.Configuration.MapR.Raw.OVERWRITE);
            int bufferSize = configuration.getInt(com.attensity.configuration.Configuration.MapR.Raw.BUFFER_SIZE);
            int replication = configuration.getInt(com.attensity.configuration.Configuration.MapR.Raw.REPLICATION);
            long blockSize = configuration.getLong(com.attensity.configuration.Configuration.MapR.Raw.BLOCK_SIZE);

            conf = new Configuration();
//            fileSystem = FileSystem.get(conf);
//            fileSystem = DistributedFileSystem.get(conf);
//            fileSystem = new MapRFileSystem();//MapRFileSystem.get(conf);
            fileSystem = MapRFileSystem.get(conf);
//            fileSystem.setConf(conf);
            dirPath = new Path(dirName + "/dir");
            wFilePath = new Path(dirName + "/file.w");
            //rFilePath = wFilePath;//new Path(DIR_NAME + "file.r");

            if (fileSystem.exists(wFilePath)) {
                fileSystem.delete(wFilePath, true);
            }

            outputStream = fileSystem.create(wFilePath);//,
//                                             overwrite,
//                                             bufferSize,
//                                             (short) replication,
//                                             blockSize);

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
                LOGGER.error("Unknown error closing MapR output stream.", e);
            }
        }

        if (null != fileSystem) {
            try {
                fileSystem.close();
                LOGGER.info("Finished writing to MapR.");
            } catch (IOException e) {
                LOGGER.error("Unable to close filesystem.", e);
            } catch (Exception e) {
                LOGGER.error("Unknown error closing filesystem.", e);
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