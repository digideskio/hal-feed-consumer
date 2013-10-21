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

    public FeedConsumerScheduler(final FeedConsumer consumer, final Interval interval)
    {
        this(consumer, interval, newSingleThreadScheduledExecutor());
    }

    FeedConsumerScheduler(final FeedConsumer consumer, final Interval interval, final ScheduledExecutorService scheduledExecutorService)
    {
        this.consumer = consumer;
        this.interval = interval;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void start()
    {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                consume();
            }
        }, 0, interval.time, interval.unit);
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
            LOG.error("Caught feed-consumer exception", e);
        }
    }
}
