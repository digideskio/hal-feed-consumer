package com.qmetric.feed.consumer;

import org.joda.time.DateTime;

import static org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals;
import static org.apache.commons.lang3.builder.HashCodeBuilder.reflectionHashCode;
import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;

public class SeenEntry
{
    public final EntryId id;

    public final DateTime dateTime;

    private final boolean alreadyTracked;

    public SeenEntry(final EntryId id, final DateTime dateTime)
    {
        this(id, dateTime, false);
    }

    public SeenEntry(final EntryId id, final DateTime dateTime, final boolean alreadyTracked)
    {
        this.id = id;
        this.dateTime = dateTime;
        this.alreadyTracked = alreadyTracked;
    }

    public boolean alreadyTracked()
    {
        return alreadyTracked;
    }

    public boolean notAlreadyTracked()
    {
        return !alreadyTracked();
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
        return reflectionToString(this);
    }
}