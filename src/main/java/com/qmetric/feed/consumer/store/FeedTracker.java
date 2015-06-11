package com.qmetric.feed.consumer.store;

import com.qmetric.feed.consumer.EntryId;
import com.qmetric.feed.consumer.SeenEntry;
import com.qmetric.feed.consumer.TrackedEntry;

public interface FeedTracker
{
    void checkConnectivity() throws ConnectivityException;

    boolean isTracked(EntryId id);

    void track(SeenEntry entry);

    Iterable<TrackedEntry> getEntriesToBeConsumed();

    void markAsConsuming(EntryId id) throws AlreadyConsumingException;

    void markAsConsumed(EntryId id);

    void fail(TrackedEntry trackedEntry, boolean scheduleRetry);
}
