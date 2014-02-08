package com.qmetric.feed.consumer;

import com.theoryinpractise.halbuilder.api.ReadableRepresentation;

import static org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals;
import static org.apache.commons.lang3.builder.HashCodeBuilder.reflectionHashCode;

public class FeedEntry
{
    public final ReadableRepresentation content;

    public final int retries;

    public FeedEntry(final ReadableRepresentation content, final int retries)
    {
        this.content = content;
        this.retries = retries;
    }

    @Override public boolean equals(final Object obj)
    {
        return reflectionEquals(this, obj);
    }

    @Override public int hashCode()
    {
        return reflectionHashCode(this);
    }
}
