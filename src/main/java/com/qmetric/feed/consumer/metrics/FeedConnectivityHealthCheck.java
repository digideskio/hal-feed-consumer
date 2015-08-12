package com.qmetric.feed.consumer.metrics;

import com.codahale.metrics.health.HealthCheck;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;

import java.net.URI;
import java.net.URISyntaxException;

import static com.codahale.metrics.health.HealthCheck.Result.healthy;
import static com.codahale.metrics.health.HealthCheck.Result.unhealthy;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;

public class FeedConnectivityHealthCheck extends HealthCheck
{
    private final String feedPingUrl;

    private final HttpClient client;

    public FeedConnectivityHealthCheck(final String feedUrl, final HttpClient client)
    {
        this.feedPingUrl = new UrlUtils().pingUrlFrom(feedUrl);
        this.client = client;
    }

    @Override protected Result check() throws Exception
    {
        final HttpGet httpGet = new HttpGet(feedPingUrl);
        final HttpResponse response = client.execute(httpGet);
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HTTP_OK)
        {
            return healthy("Ping was successful to %s", feedPingUrl);
        }
        else
        {
            return unhealthy("Unhealthy with status %s", statusCode);
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
