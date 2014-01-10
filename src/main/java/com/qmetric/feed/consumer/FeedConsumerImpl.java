package com.qmetric.feed.consumer;

import com.qmetric.feed.consumer.store.AlreadyConsumingException;
import com.qmetric.feed.consumer.store.FeedTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.FluentIterable.from;

public class FeedConsumerImpl implements FeedConsumer
{
    private static final Logger LOG = LoggerFactory.getLogger(FeedConsumerImpl.class);

    private final EntryConsumer entryConsumer;

    private final FeedTracker feedTracker;

    private final Collection<FeedPollingListener> listeners;

    public FeedConsumerImpl(final EntryConsumer entryConsumer, final FeedTracker feedTracker, final Collection<FeedPollingListener> listeners)
    {
        this.entryConsumer = entryConsumer;
        this.feedTracker = feedTracker;
        this.listeners = listeners;
    }

    @Override
    public List<EntryId> consume() throws Exception
    {
        return consume(unconsumed());
    }

    private List<EntryId> consume(final List<EntryId> entries) throws Exception
    {
        processEach(entries);

        notifyAllListeners(entries);

        return entries;
    }

    private void processEach(final List<EntryId> entries) throws Exception
    {
        for (final EntryId id : entries)
        {
            try
            {
                LOG.debug("Consuming entry {}", id);
                entryConsumer.consume(id);
            }
            catch (AlreadyConsumingException e)
            {
                LOG.info("Entry {} already being consumed", id, e);
            }
            catch (Exception e)
            {
                LOG.warn("Entry {} failed processing", id, e);
            }
        }
    }

    private List<EntryId> unconsumed()
    {
        return from(feedTracker.getItemsToBeConsumed()).toList();
    }

    private void notifyAllListeners(final List<EntryId> consumedEntries)
    {
        for (final FeedPollingListener listener : listeners)
        {
            listener.consumed(consumedEntries);
        }
    }
}
