package com.attensity.twitter;

import com.attensity.configuration.TwitterStreamConfiguration;
import com.attensity.core.AbstractFactory;
import com.twitter.hbc.BasicReconnectionManager;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.ReconnectionManager;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.DefaultStreamingEndpoint;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.typesafe.config.Config;
import org.apache.commons.lang.Validate;

import java.util.concurrent.BlockingQueue;

/**
 * @author lmedina
 */
public class ClientFactory implements AbstractFactory<Client> {
    private Config configuration;

    private BlockingQueue<String> messageQueue;

    public ClientFactory(Config configuration, BlockingQueue<String> messageQueue) {
        Validate.notNull(configuration, "configuration must not be null.");
        Validate.notNull(messageQueue, "messageQueue must not be null.");

        this.configuration = configuration;
        this.messageQueue = messageQueue;
    }

    @Override
    public Client create() {
        return new ClientBuilder().authentication(createAuthentication())
                .endpoint(createStreamingEndpoint())
                .gzipEnabled(configuration.getBoolean(TwitterStreamConfiguration.HBC.GZIP_ENABLED))
                .hosts(HttpHosts.STREAM_HOST)
                .name(configuration.getString(TwitterStreamConfiguration.HBC.CLIENT_NAME))
                .processor(new StringDelimitedProcessor(messageQueue))
                .reconnectionManager(createReconnectionManager())
                .build();
    }

    private Authentication createAuthentication() {
        return new OAuth1(configuration.getString(TwitterStreamConfiguration.Authentication.API_KEY),
                          configuration.getString(TwitterStreamConfiguration.Authentication.API_SECRET),
                          configuration.getString(TwitterStreamConfiguration.Authentication.ACCESS_TOKEN),
                          configuration.getString(TwitterStreamConfiguration.Authentication.ACCESS_TOKEN_SECRET));
    }

    private StreamingEndpoint createStreamingEndpoint() {
        return getConfiguredEndpoint(new StatusesSampleEndpoint());
    }

    private DefaultStreamingEndpoint getConfiguredEndpoint(DefaultStreamingEndpoint endpoint) {
        endpoint.setBackfillCount(Constants.MAX_BACKOFF_COUNT);
        endpoint.stallWarnings(configuration.getBoolean(TwitterStreamConfiguration.HBC.STALL_WARNINGS));
        endpoint.filterLevel(Constants.FilterLevel.None);

        return endpoint;
    }

    private ReconnectionManager createReconnectionManager() {
        return new BasicReconnectionManager(configuration.getInt(TwitterStreamConfiguration.HBC.CONNECTION_RETRIES));
    }
}
