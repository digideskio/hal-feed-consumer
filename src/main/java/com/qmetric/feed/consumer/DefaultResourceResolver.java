package com.qmetric.feed.consumer;

import com.theoryinpractise.halbuilder.DefaultRepresentationFactory;
import com.theoryinpractise.halbuilder.api.Link;
import com.theoryinpractise.halbuilder.api.ReadableRepresentation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;

public class DefaultResourceResolver implements ResourceResolver
{

    private final RepresentationFactory representationFactory;

    private final FeedEndpointFactory endpoint;

    public DefaultResourceResolver(final FeedEndpointFactory endpoint)
    {
        this.endpoint = endpoint;
        representationFactory = new DefaultRepresentationFactory();
    }

    @Override public ReadableRepresentation resolve(final Link link)
    {
        return representationFactory.readRepresentation(endpoint.create(link.getHref()).get());
    }
}
