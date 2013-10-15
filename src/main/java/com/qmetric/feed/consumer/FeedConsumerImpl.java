package com.qmetric.feed.consumer;

import com.google.common.base.Optional;
import com.qmetric.feed.consumer.store.ConsumedStore;
import com.theoryinpractise.halbuilder.api.ReadableRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

public class FeedConsumerImpl implements FeedConsumer
{

    private static final Logger log = LoggerFactory.getLogger(FeedConsumerImpl.class);

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
        return consume(unconsumed());
    }

    private List<ReadableRepresentation> consume(final List<ReadableRepresentation> entries) throws Exception
    {
        processEach(entries);

        notifyAllListeners(entries);

        return entries;
    }

    private void processEach(final List<ReadableRepresentation> entries) throws Exception
    {
        for (final ReadableRepresentation feedEntry : entries)
        {
            log.debug("Consuming entry {}", feedEntry.getResourceLink().getHref());
            entryConsumer.consume(feedEntry);
        }
    }

    private List<ReadableRepresentation> unconsumed()
    {
        return finder.findUnconsumed(endpoint);
    }

    private void notifyAllListeners(final List<ReadableRepresentation> consumedEntries)
    {
        for (final FeedPollingListener listener : listeners)
        {
            listener.consumed(consumedEntries);
        }
    }
}
