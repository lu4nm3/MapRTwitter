package com.attensity.twitter;

import com.attensity.core.StreamManager;
import com.twitter.hbc.core.Client;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lmedina
 */
public class TwitterStreamManager implements StreamManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterStreamManager.class);

    private Client client;
    protected boolean isConnected;
    private boolean permanentlyDisconnected;

    public TwitterStreamManager(Client client) {
        Validate.notNull(client, "client must not be null.");

        this.client = client;
    }

    @Override
    public void connect() {
        if (permanentlyDisconnected) {
            LOGGER.error("Data stream has been permanently disconnected. Unable to connect.");
        } else {
            if (isConnected) {
                LOGGER.warn("Already connected to the data stream.");
            } else {
                try {
                    client.connect();
                    isConnected = true;
                    LOGGER.info("Connected to the data stream.");
                } catch (Exception e) {
                    LOGGER.error("Failed to connect to the data stream.", e);
                }
            }
        }
    }

    @Override
    public void reconnect() {
        if (permanentlyDisconnected) {
            LOGGER.error("Data stream has been permanently disconnected. Unable to reconnect.");
        } else {
            if (isConnected) {
                try {
                    client.reconnect();
                    LOGGER.info("Reconnected to the data stream.");
                } catch (Exception e) {
                    LOGGER.error("Failed to reconnect to the data stream.", e);
                }
            } else {
                LOGGER.warn("Not previously connected to the data stream. Unable to reconnect.");
            }
        }
    }

    @Override
    public void disconnect() {
        if (permanentlyDisconnected) {
            LOGGER.error("Data stream has been permanently disconnected. Unable to disconnect.");
        } else {
            if (isConnected) {
                try {
                    client.stop();
                    isConnected = false;
                    permanentlyDisconnected = true;
                    LOGGER.info("Permanently disconnected from the data stream.");
                } catch (Exception e) {
                    LOGGER.error("Failed to disconnect from the data stream.", e);
                }
            } else {
                LOGGER.warn("Not connected to the data stream. Unable to disconnect.");
            }
        }
    }
}
