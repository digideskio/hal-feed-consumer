package com.qmetric.feed.consumer.multipleClientsTest

import com.qmetric.feed.consumer.store.AlreadyConsumingException
import com.qmetric.feed.consumer.store.ConnectivityException
import com.qmetric.feed.consumer.store.FeedTracker
import com.theoryinpractise.halbuilder.api.Link

class InMemoryFeedTracker implements FeedTracker
{

    def consuming = new HashSet()
    def consumed = new HashSet()

    @Override void checkConnectivity() throws ConnectivityException
    {}

    @Override void markAsConsuming(final Link feedEntry) throws AlreadyConsumingException
    {
        if (consuming.contains(feedEntry))
        {
            throw new AlreadyConsumingException()
        }
        consuming.add(feedEntry)
    }

    @Override void revertConsuming(final Link feedEntry)
    {
        consuming.remove(feedEntry)
    }

    @Override void markAsConsumed(final Link feedEntry)
    {
        consumed.add(feedEntry)
    }

    @Override boolean isTracked(final Link feedEntry)
    {
        // TODO tracked/consumed symmetry is not true anymore
        return !consumed.contains(feedEntry)
    }

    @Override void track(final Link link)
    {}

    @Override Iterable<Link> getItemsToBeConsumed()
    {
        throw new UnsupportedOperationException()
    }

    int getConsumedCount()
    {
        consumed.size()
    }

    int getConsumingCount()
    {
        consuming.size()
    }
}