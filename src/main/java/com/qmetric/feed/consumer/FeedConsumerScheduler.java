package com.qmetric.feed.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class FeedConsumerScheduler
{
    private static final Logger LOG = LoggerFactory.getLogger(FeedConsumerScheduler.class);

    private final FeedConsumer consumer;

    private final Interval interval;

    private final ScheduledExecutorService scheduledExecutorService;

    private final boolean registerShutdownHook;

    private final String feedUrl;

    private final AvailableFeedEntriesTracker feedEntriesTracker;

    private final AtomicInteger invocationCounter = new AtomicInteger(0);

    private ShutdownProcedure shutdownProcedure;

    FeedConsumerScheduler(final FeedConsumer consumer, AvailableFeedEntriesTracker feedEntriesTracker, final Interval interval, final ScheduledExecutorService scheduledExecutorService, final boolean registerShutdownHook, final String feedUrl)
    {
        this.consumer = consumer;
        this.interval = interval;
        this.scheduledExecutorService = scheduledExecutorService;
        this.registerShutdownHook = registerShutdownHook;
        this.feedUrl = feedUrl;
        this.shutdownProcedure = new ShutdownProcedure(scheduledExecutorService);
        this.feedEntriesTracker = feedEntriesTracker;
    }

    public void start()
    {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                pollFeed();
            }
        }, 0, interval.time, interval.unit);

        if (registerShutdownHook)
        {
            LOG.info("Registering shutdown hook");
            shutdownProcedure.registerShutdownHook();
        }
    }

    public void stop()
    {
        if (registerShutdownHook)
        {
            shutdownProcedure.runAndRemoveHook();
        }
        else
        {
            shutdownProcedure.run();
        }
    }

    protected int getInvocationsCount()
    {
        return invocationCounter.get();
    }

    private void pollFeed()
    {
        try
        {
            LOG.info("Checking feed {} for new entries", feedUrl);

            updateTracker();
            consume();
            invocationCounter.getAndIncrement();
        }
        catch (final Throwable e)
        {
            LOG.error("poll-feed exception", e);
        }
    }

    private void updateTracker()
    {
        try
        {
            LOG.debug("Running entry-tracker");

            feedEntriesTracker.trackNewEntries();

            LOG.debug("entry-tracker returned normally");
        }
        catch (final Exception e)
        {
            LOG.error("entry-tracker exception", e);
        }
    }

    private void consume()
    {
        try
        {
            LOG.debug("Invoking feed-consumer");

            consumer.consume();

            LOG.debug("Feed-consumer returned normally");
        }
        catch (final Exception e)
        {
            LOG.error("feed-consumer exception", e);
        }
    }
}
