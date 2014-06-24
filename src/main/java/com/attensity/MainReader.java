package com.attensity;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

/**
 * @author lmedina
 */
public class MainReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(MainReader.class);

    private Config configuration;

    // MapR
    private Configuration conf;
    private FileSystem fileSystem;
    private Path rFilePath;
    private FSDataInputStream inputStream;

    private static MainReader mainReader = new MainReader();

    public static void main(String[] args) {
        mainReader.init();
        mainReader.start();

        mainReader.stop();

        LOGGER.info("Exiting...");
        System.exit(0);
    }

    private void init() {
        configuration = ConfigFactory.parseFile(new File("configuration/application.conf"));

        initMapR();
    }

    private void initMapR() {
        conf = new Configuration();
        try {
            fileSystem = FileSystem.get(conf);
            String dirName = configuration.getString(com.attensity.configuration.Configuration.MapR.Raw.DIR_NAME);
            rFilePath = new Path(dirName + "/file.w");
            inputStream = fileSystem.open(rFilePath);
        } catch (IOException e) {
            LOGGER.error("Unable to initialize MapR input stream.", e);
        } catch (Exception e) {
            LOGGER.error("There was a problem initializing variables for MapR.", e);
        }
    }

    private void start() {
        String json = "";
        long readMessages = configuration.getLong(com.attensity.configuration.Configuration.READ_MESSAGES);
        long messageCount = 0;

        try {
            for (int i = 0; i < readMessages; i++) {
                json = inputStream.readUTF();
                System.out.println(json);
                messageCount++;
            }
        } catch (EOFException e) {
            LOGGER.error("Input stream reaches the end before reading all the bytes.", e);
        } catch (IOException e) {
            LOGGER.error("Input stream was closed in the middle of reading.", e);
        } catch (Exception e) {
            LOGGER.error("The bytes do not represent a valid modified UTF-8 encoding of a string.", e);
        }

        if (null != inputStream) {
            try {
                inputStream.close();
                LOGGER.info("Finished reading from MapR.");
            } catch (IOException e) {
                LOGGER.error("Unable to close MapR input stream.", e);
            } catch (Exception e) {
                LOGGER.error("Unknown error closing MapR input stream.", e);
            }
        }

        LOGGER.info("Total number of messages read - " + messageCount);
    }

    private void stop() {

    }
}
