package com.qmetric.feed.consumer;

import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.client.Client;
import java.util.concurrent.TimeUnit;

public class FeedEndpointFactory
{
    private final Client client;

    public FeedEndpointFactory(final Client client, final ConnectionTimeout timeout)
    {
        this.client = client;
        initClient(timeout);
    }

    private void initClient(final ConnectionTimeout timeout)
    {
        client.property(ClientProperties.CONNECT_TIMEOUT, timeout.asMillis());
        client.property(ClientProperties.READ_TIMEOUT, timeout.asMillis());
    }

    public FeedEndpoint create(final String url)
    {
        return new FeedEndpoint(client.target(url));
    }

    public static class ConnectionTimeout
    {
        final TimeUnit unit;

        final int value;

        public ConnectionTimeout(final TimeUnit unit, final int value)
        {
            this.unit = unit;
            this.value = value;
        }

        public int asMillis()
        {
            return (int) unit.toMillis(value);
        }
    }
}
