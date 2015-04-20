package com.qmetric.feed.consumer;


import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import static com.google.common.base.Preconditions.checkState;
import static com.theoryinpractise.halbuilder.api.RepresentationFactory.HAL_JSON;
import static javax.ws.rs.core.Response.Status.OK;

public class FeedEndpoint
{
    private final WebTarget target;

    public FeedEndpoint(final WebTarget target)
    {
        this.target = target;
    }

    public Reader get()
    {
        return new InputStreamReader(getResponse().readEntity(InputStream.class));
    }

    private Response getResponse()
    {
        final Response response = target.request(HAL_JSON)
                .get();

        final Response.Status status = Response.Status.fromStatusCode(response.getStatus());
        check(status);

        return response;
    }

    private void check(final Response.Status status)
    {
        checkState(status == OK, "Endpoint returned [%s: %s]", status.getStatusCode(), status.getReasonPhrase());
    }
}
