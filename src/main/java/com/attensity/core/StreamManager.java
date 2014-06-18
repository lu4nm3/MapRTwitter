package com.attensity.core;

/**
 * @author lmedina
 */
public interface StreamManager {
    void connect();
    void reconnect();
    void disconnect();
}
