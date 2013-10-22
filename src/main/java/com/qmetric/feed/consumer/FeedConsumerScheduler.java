package com.qmetric.feed.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class FeedConsumerScheduler
{
    private static final Logger LOG = LoggerFactory.getLogger(FeedConsumerScheduler.class);

    private final FeedConsumer consumer;

    private final Interval interval;

    private final ScheduledExecutorService scheduledExecutorService;

    private final AvailableFeedEntriesFinder feedEntriesFinder;

    public FeedConsumerScheduler(final FeedConsumer consumer, AvailableFeedEntriesFinder feedEntriesFinder, final Interval interval)
    {
        this(consumer, feedEntriesFinder, interval, newSingleThreadScheduledExecutor());
    }

    FeedConsumerScheduler(final FeedConsumer consumer, AvailableFeedEntriesFinder feedEntriesFinder, final Interval interval,
                          final ScheduledExecutorService scheduledExecutorService)
    {
        this.consumer = consumer;
        this.interval = interval;
        this.scheduledExecutorService = scheduledExecutorService;
        this.feedEntriesFinder = feedEntriesFinder;
    }

    public void start()
    {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                updateTracker();
            }
        }, 0, interval.time, interval.unit);

        scheduledExecutorService.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                consume();
            }
        }, interval.time / 2, interval.time, interval.unit);
    }

    private void updateTracker()
    {
        try
        {
            LOG.info("Running entry-finder");

            feedEntriesFinder.findNewEntries();

            LOG.info("entry-finder returned normally");
        }
        catch (final Exception e)
        {
            LOG.error("entry-finder exception", e);
        }
    }

    private void consume()
    {
        try
        {
            LOG.info("Invoking feed-consumer");

            consumer.consume();

            LOG.info("Feed-consumer returned normally");
        }
        catch (final Exception e)
        {
            LOG.error("feed-consumer exception", e);
        }
    }
}
