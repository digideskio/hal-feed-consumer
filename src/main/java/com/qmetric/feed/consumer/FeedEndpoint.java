package com.qmetric.feed.consumer;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import java.io.InputStreamReader;
import java.io.Reader;

import static com.google.common.base.Preconditions.checkState;
import static com.sun.jersey.api.client.ClientResponse.Status.OK;
import static com.theoryinpractise.halbuilder.api.RepresentationFactory.HAL_JSON;

public class FeedEndpoint
{

    private final WebResource resource;

    public FeedEndpoint(final WebResource resource)
    {
        this.resource = resource;
    }

    public Reader get()
    {
        return new InputStreamReader(getClientResponse().getEntityInputStream());
    }

    private ClientResponse getClientResponse()
    {
        final ClientResponse clientResponse = resource.accept(HAL_JSON).get(ClientResponse.class);
        check(clientResponse.getClientResponseStatus());
        return clientResponse;
    }

    private void check(final ClientResponse.Status status)
    {
        checkState(status == OK, "Endpoint returned [%s: %s]", status.getStatusCode(), status.getReasonPhrase());
    }
}
