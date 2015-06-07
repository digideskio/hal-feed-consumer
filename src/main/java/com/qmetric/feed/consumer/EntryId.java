package com.qmetric.feed.consumer;

import static org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals;
import static org.apache.commons.lang3.builder.HashCodeBuilder.reflectionHashCode;

public class EntryId
{
    private final String id;

    private EntryId(final String id)
    {
        this.id = id;
    }

    public static EntryId of(final String id)
    {
        return new EntryId(id);
    }

    public long asNumeric()
    {
        return Long.valueOf(id);
    }

    @Override public boolean equals(final Object obj)
    {
        return reflectionEquals(this, obj);
    }

    @Override public int hashCode()
    {
        return reflectionHashCode(this);
    }

    @Override public String toString()
    {
        return id;
    }
}
