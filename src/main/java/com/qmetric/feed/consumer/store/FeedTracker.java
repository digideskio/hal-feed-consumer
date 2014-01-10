package com.qmetric.feed.consumer.store;

import com.qmetric.feed.consumer.EntryId;

public interface FeedTracker
{
    void checkConnectivity() throws ConnectivityException;

    void markAsConsuming(final EntryId id) throws AlreadyConsumingException;

    void revertConsuming(final EntryId id);

    void fail(final EntryId id);

    void markAsConsumed(EntryId id);

    boolean isTracked(EntryId id);

    void track(EntryId id);

    Iterable<EntryId> getItemsToBeConsumed();
}
