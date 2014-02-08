package com.qmetric.feed.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class FeedConsumerScheduler
{
    private static final Logger LOG = LoggerFactory.getLogger(FeedConsumerScheduler.class);

    private final FeedConsumer consumer;

    private final Interval interval;

    private final ScheduledExecutorService scheduledExecutorService;

    private final AvailableFeedEntriesFinder feedEntriesFinder;

    private final AtomicInteger invocationCounter = new AtomicInteger(0);

    private ShutdownProcedure shutdownProcedure;

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
        shutdownProcedure = new ShutdownProcedure(scheduledExecutorService);
        this.feedEntriesFinder = feedEntriesFinder;
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
        shutdownProcedure.registerShutdownHook();
    }

    public void stop()
    {
        shutdownProcedure.runAndRemoveHook();
    }

    protected int getInvocationsCount()
    {
        return invocationCounter.get();
    }

    private void pollFeed()
    {
        try
        {
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
            LOG.info("Running entry-finder");

            feedEntriesFinder.trackNewEntries();

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
