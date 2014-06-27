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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
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
    public static final String MAPR_TABLE_NAME = "/mapr/MapR_EMR.amazonaws.com/pipeline/tweets";
    final String TWITTER="EEE MMM dd HH:mm:ss ZZZZZ yyyy";

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

    // Mapr Table
    private ObjectMapper mapper = new ObjectMapper();
    HTable table;
    Configuration hBaseConfiguration;

//    private FSDataInputStream inputStream;

    public MapRWriter(Config configuration, AtomicLong messageCount, BlockingQueue<String> twitterMessageQueue, WriteMode writeMode) {
        this.configuration = configuration;
        this.messageCount = messageCount;
        this.twitterMessageQueue = twitterMessageQueue;
        this.writeMode = writeMode;

        try {
            if(this.writeMode.getValue().startsWith("mapRHive")) {
                LOGGER.info("Initializing mapr table...");
                initMapRTable();
            }
            else if (this.writeMode.getValue().contains("mapR")) {
                LOGGER.info("Initializing mapr-fs...");
                initMapR();
            }
        }
        catch(Exception e) {
            LOGGER.error("Error initializing with configuration writemode: " + writeMode);
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

    private void initMapRTable() {
        try {
            hBaseConfiguration = HBaseConfiguration.create();
            hBaseConfiguration.set("hbase.table.namespace.mappings","*:pipeline");
            hBaseConfiguration.set("mapr.htable.impl","com.mapr.fs.MapRHTable");
            hBaseConfiguration.set("fs.default.name", "maprfs://MapR_EMR.amazonaws.com ip-10-72-222-97.ec2.internal:7222 ip-10-72-207-57.ec2.internal:7222");
            hBaseConfiguration.set("fs.maprfs.impl", "com.mapr.fs.MapRFileSystem");
            table = new HTable(hBaseConfiguration, MAPR_TABLE_NAME);

            LOGGER.info("Table configuration: " + table.getConfiguration().get("mapr.htable.impl"));

        } catch (Exception e) {
            LOGGER.error("Error connecting to the mapr table: ", e);
        }
        LOGGER.info("Done initialing......");
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
                            writeRawUncompressedToMapRTable(json);
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

        if(table != null) {
            try {
                table.close();
            } catch (IOException e) {
                LOGGER.error("Error closing HTable object", e);
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

    private void writeRawUncompressedToMapRTable(String jsonString) {
        LOGGER.info("Twitter Message - " + jsonString);

        String idString = null;

        try {
            Map<String, Object> messageMap = createMessageMap(jsonString);
            Object id = messageMap != null ? messageMap.get("id_str") : null;
            Object text = messageMap != null? messageMap.get("text") : null;
            Object createdDate = messageMap != null? messageMap.get("created_at") : null;

            idString = id != null ? (String)id : null;
            String textAsString = text != null? (String)text : null;
            Date createdAt = getTwitterDate((String)createdDate);

            if(id != null) {
                LOGGER.info(String.format("Adding: id: %s, text: %s, dt: %s, all: %s", idString, textAsString, createdAt.getTime(), jsonString));

                Put p = new Put(Bytes.toBytes(idString));
                p.add(Bytes.toBytes("all"), Bytes.toBytes("all"),Bytes.toBytes(jsonString));
                p.add(Bytes.toBytes("text"), Bytes.toBytes("text"),Bytes.toBytes(textAsString));
                p.add(Bytes.toBytes("dt"), Bytes.toBytes("dt"),Bytes.toBytes(createdAt.getTime()));

                table.put(p);
            }
        }
        catch(Exception e) {
            LOGGER.error(String.format("Error occurred saving id: %s.", idString), e);
        }
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

    public Date getTwitterDate(String date) throws ParseException {
        if(date != null) {
            SimpleDateFormat sf = new SimpleDateFormat(TWITTER);
            sf.setLenient(true);
            return sf.parse(date);
        }
        return null;
    }
}