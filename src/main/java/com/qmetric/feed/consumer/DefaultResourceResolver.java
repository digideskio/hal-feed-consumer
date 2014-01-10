package com.qmetric.feed.consumer;

import com.theoryinpractise.halbuilder.api.ReadableRepresentation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;

import static java.lang.String.format;

public class DefaultResourceResolver implements ResourceResolver
{
    private static final String FEED_ENTRY_URL_TEMPLATE = "%s/%s";

    private final String feedUrl;

    private final RepresentationFactory representationFactory;

    private final FeedEndpointFactory endpoint;

    public DefaultResourceResolver(final String feedUrl, final FeedEndpointFactory endpoint, final RepresentationFactory representationFactory)
    {
        this.feedUrl = feedUrl;
        this.endpoint = endpoint;
        this.representationFactory = representationFactory;
    }

    @Override public ReadableRepresentation resolve(final EntryId id)
    {
        return representationFactory.readRepresentation(endpoint.create(buildUrlToFeedEntry(id)).get());
    }

    private String buildUrlToFeedEntry(final EntryId id)
    {
        return format(FEED_ENTRY_URL_TEMPLATE, feedUrl, id);
    }
}
