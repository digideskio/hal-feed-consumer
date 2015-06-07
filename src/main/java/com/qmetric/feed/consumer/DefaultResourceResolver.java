package com.qmetric.feed.consumer;

import com.google.common.base.Optional;
import com.qmetric.hal.reader.HalReader;
import com.qmetric.hal.reader.HalResource;

import java.io.Reader;

import static java.lang.String.format;

public class DefaultResourceResolver implements ResourceResolver
{
    private static final String FEED_ENTRY_URL_TEMPLATE = "%s/%s";

    private final String feedUrl;

    private final HalReader halReader;

    private final FeedEndpointFactory endpoint;

    public DefaultResourceResolver(final String feedUrl, final FeedEndpointFactory endpoint, final HalReader halReader)
    {
        this.feedUrl = feedUrl;
        this.endpoint = endpoint;
        this.halReader = halReader;
    }

    @Override public HalResource resolve(final EntryId id)
    {
        return halReader.read(endpoint.create(buildUrlToFeedEntry(id)).get());
    }

    private String buildUrlToFeedEntry(final EntryId id)
    {
        return format(FEED_ENTRY_URL_TEMPLATE, feedUrl, id);
    }
}
