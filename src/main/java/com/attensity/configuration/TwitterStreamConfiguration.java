package com.attensity.configuration;

/**
 * @author lmedina
 */
public class TwitterStreamConfiguration {
    public static class Authentication {
        public static final String ACCESS_TOKEN = "stream.twitter.authentication.accessToken";
        public static final String ACCESS_TOKEN_SECRET = "stream.twitter.authentication.accessTokenSecret";
        public static final String API_KEY = "stream.twitter.authentication.apiKey";
        public static final String API_SECRET = "stream.twitter.authentication.apiSecret";
    }

    public static final String DATA_STREAM = "stream.twitter.dataStream";

    public static class HBC {
        public static final String CLIENT_NAME = "stream.twitter.hbc.clientName";
        public static final String CONNECTION_RETRIES = "stream.twitter.hbc.connectionRetries";
        public static final String GZIP_ENABLED = "stream.twitter.hbc.gzipEnabled";
        public static final String STALL_WARNINGS = "stream.twitter.hbc.stallWarnings";
    }
}
