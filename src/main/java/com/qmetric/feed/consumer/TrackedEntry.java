package com.qmetric.feed.consumer;

import org.joda.time.DateTime;

import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;

public class TrackedEntry
{
    public final EntryId id;

    public final DateTime created;

    public final int retries;

    public TrackedEntry(final EntryId id, final DateTime created, final int retries)
    {
        this.id = id;
        this.created = created;
        this.retries = retries;
    }

    @Override public boolean equals(final Object obj)
    {
        return id.equals(obj);
    }

    @Override public int hashCode()
    {
        return id.hashCode();
    }

    @Override public String toString()
    {
        return reflectionToString(this);
    }
}
