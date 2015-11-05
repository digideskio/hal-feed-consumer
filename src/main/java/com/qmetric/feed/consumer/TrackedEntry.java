package com.qmetric.feed.consumer;

import com.qmetric.feed.consumer.retry.RetryStrategy;
import org.joda.time.DateTime;

import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;

public class TrackedEntry
{
    public final EntryId id;

    public final DateTime created;

    public final int retries;

    private final DateTime seenAt;

    public TrackedEntry(final EntryId id, final DateTime created, final DateTime seenAt, final int retries)
    {
        this.id = id;
        this.created = created;
        this.retries = retries;
        this.seenAt = seenAt;
    }

    @Deprecated
    public TrackedEntry(final EntryId id, final DateTime created, final int retries)
    {
        this(id, created, null, retries);
    }

    public boolean canBeProcessed(RetryStrategy retryStrategy, DateTime currentTime) {
        return canBeAlwaysProcessed() || canBeProcessedAccordingToStrategy(retryStrategy, currentTime);
    }

    private boolean canBeAlwaysProcessed() {
        return seenAt == null || retries == 0;
    }

    private boolean canBeProcessedAccordingToStrategy(RetryStrategy retryStrategy, DateTime currentTime) {
        return retryStrategy.canRetry(retries, seenAt, currentTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TrackedEntry that = (TrackedEntry) o;

        return !(id != null ? !id.equals(that.id) : that.id != null);

    }

    @Deprecated
    public boolean equals(final EntryId obj)
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
