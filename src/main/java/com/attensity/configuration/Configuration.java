package com.attensity.configuration;

/**
 * @author lmedina
 */
public class Configuration {
    public static class Stream {
        public static class Twitter {
            public static class Authentication {
                public static final String ACCESS_TOKEN = "stream.twitter.authentication.accessToken";
                public static final String ACCESS_TOKEN_SECRET = "stream.twitter.authentication.accessTokenSecret";
                public static final String API_KEY = "stream.twitter.authentication.apiKey";
                public static final String API_SECRET = "stream.twitter.authentication.apiSecret";
            }

            public static class HBC {
                public static final String CLIENT_NAME = "stream.twitter.hbc.clientName";
                public static final String CONNECTION_RETRIES = "stream.twitter.hbc.connectionRetries";
                public static final String GZIP_ENABLED = "stream.twitter.hbc.gzipEnabled";
                public static final String STALL_WARNINGS = "stream.twitter.hbc.stallWarnings";
            }
        }
    }

    public static final String WRITE_MODE = "writeMode";
    public static final String MAX_MESSAGES = "maxMessages";
    public static final String NUM_WRITERS = "numberOfWriters";

    public static final class MapR {
        public static final class Raw {
            public static final String DIR_NAME = "mapR.raw.dirName";
        }
    }
}
