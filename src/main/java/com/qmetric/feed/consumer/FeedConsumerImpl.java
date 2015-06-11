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
    public List<TrackedEntry> consume() throws Exception
    {
        return consume(unconsumed());
    }

    private List<TrackedEntry> consume(final List<TrackedEntry> entries) throws Exception
    {
        processEach(entries);

        notifyAllListeners(entries);

        return entries;
    }

    private void processEach(final List<TrackedEntry> entries) throws Exception
    {
        for (final TrackedEntry trackedEntry : entries)
        {
            try
            {
                LOG.info("Consuming entry {}", trackedEntry);
                entryConsumer.consume(trackedEntry);
            }
            catch (AlreadyConsumingException e)
            {
                LOG.info("Entry {} already being consumed", trackedEntry, e);
            }
            catch (Exception e)
            {
                LOG.warn("Entry {} failed processing", trackedEntry, e);
            }
            catch (Throwable e)
            {
                LOG.error("Fatal error processing entry {}", trackedEntry, e);
            }
        }
    }

    private List<TrackedEntry> unconsumed()
    {
        return from(feedTracker.getEntriesToBeConsumed()).toList();
    }

    private void notifyAllListeners(final List<TrackedEntry> consumedEntries)
    {
        for (final FeedPollingListener listener : listeners)
        {
            listener.consumed(consumedEntries);
        }
    }
}
