package com.qmetric.feed.consumer;

import com.qmetric.feed.consumer.store.AlreadyConsumingException;
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
            LOG.info("Attempting to consume feed");

            consumer.consume();

            LOG.info("Feed consumed successfully");
        }
        catch (final AlreadyConsumingException e)
        {
            LOG.info("Entry in feed already being consumed by another consumer...skipping");
        }
        catch (final Throwable e)
        {
            LOG.error("Failed to consume feed", e);
        }
    }
}
