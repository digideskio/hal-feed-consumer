package com.qmetric.feed.consumer;

import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;

public class TrackedEntry
{
    public final EntryId id;

    public final Integer failureAttempts;

    public TrackedEntry(final EntryId id, final Integer failureAttempts)
    {
        this.id = id;
        this.failureAttempts = failureAttempts;
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
