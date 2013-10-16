package com.qmetric.feed.consumer.multipleClientsTest

import com.qmetric.feed.consumer.store.AlreadyConsumingException
import com.qmetric.feed.consumer.store.ConnectivityException
import com.qmetric.feed.consumer.store.ConsumedStore
import com.theoryinpractise.halbuilder.api.ReadableRepresentation

import static com.google.common.collect.Sets.difference

class InMemoryConsumedStore implements ConsumedStore
{

    def consuming = new HashSet()
    def consumed = new HashSet()

    @Override void checkConnectivity() throws ConnectivityException
    {}

    @Override void markAsConsuming(final ReadableRepresentation feedEntry) throws AlreadyConsumingException
    {
        if (consuming.contains(feedEntry))
        {
            throw new AlreadyConsumingException()
        }
        consuming.add(feedEntry)
    }

    @Override void revertConsuming(final ReadableRepresentation feedEntry)
    {
        consuming.remove(feedEntry)
    }

    @Override void markAsConsumed(final ReadableRepresentation feedEntry)
    {
        consumed.add(feedEntry)
    }

    @Override boolean notAlreadyConsumed(final ReadableRepresentation feedEntry)
    {
        return !consumed.contains(feedEntry)
    }

    int getConsumedCount()
    {
        consumed.size()
    }

    int getConsumingCount()
    {
        consuming.size()
    }

    boolean hasUnconsumedEntries()
    {
        difference(consuming, consumed).size() > 0
    }
}