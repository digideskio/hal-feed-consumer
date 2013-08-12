package com.qmetric.feed.consumer;

import com.google.common.base.Optional;
import com.qmetric.feed.consumer.store.ConsumedStore;
import com.theoryinpractise.halbuilder.api.ReadableRepresentation;

import java.util.Collection;
import java.util.List;

public class FeedConsumerImpl implements FeedConsumer
{
    private final FeedEndpoint endpoint;

    private final UnconsumedFeedEntriesFinder finder;

    private final EntryConsumer entryConsumer;

    private final Collection<FeedPollingListener> listeners;

    public FeedConsumerImpl(final String feedUrl, final FeedEndpointFactory endpointFactory, final EntryConsumer entryConsumer, final ConsumedStore consumedStore,
                            final Optional<EarliestEntryLimit> earliestEntryLimit, final Collection<FeedPollingListener> listeners)
    {
        this.entryConsumer = entryConsumer;
        this.listeners = listeners;
        this.endpoint = endpointFactory.create(feedUrl);
        this.finder = new UnconsumedFeedEntriesFinder(endpointFactory, consumedStore, earliestEntryLimit);
    }

    @Override
    public List<ReadableRepresentation> consume() throws Exception
    {
        final List<ReadableRepresentation> entries = finder.findUnconsumed(endpoint);

        for (final ReadableRepresentation feedEntry : entries)
        {
            entryConsumer.consume(feedEntry);
        }

        notifyAllListeners(entries);

        return entries;
    }

    private void notifyAllListeners(final List<ReadableRepresentation> consumedEntries)
    {
        for (final FeedPollingListener listener : listeners)
        {
            listener.consumed(consumedEntries);
        }
    }
}
