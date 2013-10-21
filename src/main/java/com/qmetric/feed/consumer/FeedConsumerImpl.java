package com.qmetric.feed.consumer;

import com.qmetric.feed.consumer.store.AlreadyConsumingException;
import com.qmetric.feed.consumer.store.FeedTracker;
import com.theoryinpractise.halbuilder.api.Link;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.FluentIterable.from;

public class FeedConsumerImpl implements FeedConsumer
{

    private static final Logger log = LoggerFactory.getLogger(FeedConsumerImpl.class);

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
    public List<Link> consume() throws Exception
    {
        return consume(unconsumed());
    }

    private List<Link> consume(final List<Link> entries) throws Exception
    {
        processEach(entries);

        notifyAllListeners(entries);

        return entries;
    }

    private void processEach(final List<Link> entries) throws Exception
    {
        for (final Link feedEntry : entries)
        {
            try
            {
                log.debug("Consuming entry {}", getHref(feedEntry));
                entryConsumer.consume(feedEntry);
            }
            catch (AlreadyConsumingException e)
            {
                log.info("Entry {} already being consumed", getHref(feedEntry), e);
            }
            catch (Exception e)
            {
                log.warn("Entry {} failed processing", getHref(feedEntry), e);
            }
        }
    }

    private String getHref(final Link feedEntry)
    {
        return feedEntry.getHref();
    }

    private List<Link> unconsumed()
    {
        return from(feedTracker.getItemsToBeConsumed()).toList();
    }

    private void notifyAllListeners(final List<Link> consumedEntries)
    {
        for (final FeedPollingListener listener : listeners)
        {
            listener.consumed(consumedEntries);
        }
    }
}
