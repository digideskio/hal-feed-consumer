package com.qmetric.feed.consumer.metrics;

import com.codahale.metrics.health.HealthCheck;
import org.glassfish.jersey.client.ClientResponse;

import javax.ws.rs.client.Client;
import java.net.URI;
import java.net.URISyntaxException;

import static com.codahale.metrics.health.HealthCheck.Result.healthy;
import static com.codahale.metrics.health.HealthCheck.Result.unhealthy;
import static com.theoryinpractise.halbuilder.api.RepresentationFactory.HAL_JSON;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;

public class FeedConnectivityHealthCheck extends HealthCheck
{
    private final String feedPingUrl;

    private final Client client;

    public FeedConnectivityHealthCheck(final String feedUrl, final Client client)
    {
        this.feedPingUrl = new UrlUtils().pingUrlFrom(feedUrl);
        this.client = client;
    }

    @Override protected Result check() throws Exception
    {
        final ClientResponse clientResponse = client.target(feedPingUrl).request(HAL_JSON).get(ClientResponse.class);

        if (clientResponse.getStatus() == HTTP_OK)
        {
            return healthy("Ping was successful to %s", feedPingUrl);
        }
        else
        {
            return unhealthy("Unhealthy with status %s", clientResponse.getStatus());
        }
    }

    static class UrlUtils
    {
        String pingUrlFrom(final String url)
        {
            try
            {
                final URI uri = new URI(url);

                return uri.getPort() > 0 ? format("%s://%s:%s/ping", uri.getScheme(), uri.getHost(), uri.getPort()) : format("%s://%s/ping", uri.getScheme(), uri.getHost());
            }
            catch (URISyntaxException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
