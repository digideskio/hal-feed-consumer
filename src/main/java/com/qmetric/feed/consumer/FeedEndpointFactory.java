package com.qmetric.feed.consumer;

import org.apache.http.client.HttpClient;

public class FeedEndpointFactory
{
    private final HttpClient client;

    public FeedEndpointFactory(final HttpClient client)
    {
        this.client = client;
    }

    public FeedEndpoint create(final String url)
    {
        return new FeedEndpoint(client, url);
    }
}
