package com.qmetric.feed.consumer;

import com.google.common.base.Optional;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkState;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.commons.lang3.StringUtils.containsIgnoreCase;

public class FeedEndpoint
{
    private static final String EXPECTED_NOT_FOUND_BODY = "Feed entry not found";

    private final HttpClient client;

    private final String url;

    public FeedEndpoint(final HttpClient client, final String url)
    {
        this.client = client;
        this.url = url;
    }

    public Optional<Reader> get()
    {
        HttpResponse response = null;
        try
        {
            final HttpGet httpGet = new HttpGet(url);
            response = client.execute(httpGet);
            final int statusCode = response.getStatusLine().getStatusCode();

            check(response.getStatusLine());
            if (statusCode == HTTP_NOT_FOUND)
            {
                final String notFoundBody = EntityUtils.toString(response.getEntity());
                checkState(containsIgnoreCase(notFoundBody, EXPECTED_NOT_FOUND_BODY), "unexpected not found body: %s", notFoundBody);
                return Optional.absent();
            }

            String entity = EntityUtils.toString(response.getEntity(), Charset.forName("UTF-8"));
            return Optional.<Reader>of(new StringReader(entity));
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error getting HAL feed: ", e);
        }
        finally
        {
            if (response != null)
            {
                EntityUtils.consumeQuietly(response.getEntity());
            }
        }
    }

    private void check(final StatusLine status)
    {
        checkState(status.getStatusCode() == HTTP_OK || status.getStatusCode() == HTTP_NOT_FOUND, "Endpoint returned [%s: %s]", status.getStatusCode(), status.getReasonPhrase());
    }
}
