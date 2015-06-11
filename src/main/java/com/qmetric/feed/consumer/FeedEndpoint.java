package com.qmetric.feed.consumer;

import com.google.common.base.Optional;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import static com.google.common.base.Preconditions.checkState;
import static com.theoryinpractise.halbuilder.api.RepresentationFactory.HAL_JSON;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static org.apache.commons.lang3.StringUtils.containsIgnoreCase;

public class FeedEndpoint
{
    private static final String EXPECTED_NOT_FOUND_BODY = "Feed entry not found";

    private final WebTarget target;

    public FeedEndpoint(final WebTarget target)
    {
        this.target = target;
    }

    public Optional<Reader> get()
    {
        final Response response = getResponse();

        if (response.getStatus() == NOT_FOUND.getStatusCode())
        {
            final String notFoundBody = response.readEntity(String.class);

            checkState(containsIgnoreCase(notFoundBody, EXPECTED_NOT_FOUND_BODY), "unexpected not found body: %s", notFoundBody);

            return Optional.absent();
        }

        return Optional.<Reader>of(new InputStreamReader(response.readEntity(InputStream.class)));
    }

    private Response getResponse()
    {
        final Response response = target.request(HAL_JSON).get();

        final Response.Status status = Response.Status.fromStatusCode(response.getStatus());
        check(status);

        return response;
    }

    private void check(final Response.Status status)
    {
        checkState(status == OK || status == NOT_FOUND, "Endpoint returned [%s: %s]", status.getStatusCode(), status.getReasonPhrase());
    }
}
