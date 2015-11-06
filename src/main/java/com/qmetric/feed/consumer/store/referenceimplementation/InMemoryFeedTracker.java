package com.qmetric.feed.consumer.store.referenceimplementation;

import com.qmetric.feed.consumer.EntryId;
import com.qmetric.feed.consumer.SeenEntry;
import com.qmetric.feed.consumer.TrackedEntry;
import com.qmetric.feed.consumer.store.AlreadyConsumingException;
import com.qmetric.feed.consumer.store.ConnectivityException;
import com.qmetric.feed.consumer.store.FeedTracker;

public class InMemoryFeedTracker implements FeedTracker {
    @Override
    public void checkConnectivity() throws ConnectivityException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean isTracked(EntryId id) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void track(SeenEntry entry) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterable<TrackedEntry> getEntriesToBeConsumed() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void markAsConsuming(EntryId id) throws AlreadyConsumingException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void markAsConsumed(EntryId id) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void fail(TrackedEntry trackedEntry, boolean scheduleRetry) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
