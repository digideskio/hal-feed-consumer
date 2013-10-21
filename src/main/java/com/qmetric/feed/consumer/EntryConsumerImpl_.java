package com.qmetric.feed.consumer;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryerBuilder;
import com.qmetric.feed.consumer.store.AlreadyConsumingException;
import com.qmetric.feed.consumer.store.FeedTracker;
import com.theoryinpractise.halbuilder.api.Link;
import com.theoryinpractise.halbuilder.api.ReadableRepresentation;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static com.github.rholder.retry.StopStrategies.stopAfterAttempt;
import static com.github.rholder.retry.WaitStrategies.fixedWait;
import static java.util.concurrent.TimeUnit.SECONDS;

public class EntryConsumerImpl_ implements EntryConsumer_
{
    private static final RetryerBuilder<Void> RETRY_BUILDER = RetryerBuilder.<Void>newBuilder() //
            .retryIfException() //
            .withWaitStrategy(fixedWait(1, SECONDS)) //
            .withStopStrategy(stopAfterAttempt(60));

    private final FeedTracker feedTracker;

    private final ConsumeAction consumeAction;

    private final ResourceResolver resourceResolver;

    private final Collection<EntryConsumerListener_> listeners;

    public EntryConsumerImpl_(final FeedTracker feedTracker, final ConsumeAction consumeAction, final ResourceResolver resourceResolver,
                              final Collection<EntryConsumerListener_> listeners)
    {
        this.feedTracker = feedTracker;
        this.consumeAction = consumeAction;
        this.resourceResolver = resourceResolver;
        this.listeners = listeners;
    }

    @Override
    public void consume(Link link) throws Exception
    {
        markAsConsuming(link);

        process(link);

        markAsConsumed(link);

        notifyAllListeners(link);
    }

    private void markAsConsuming(final Link link) throws AlreadyConsumingException
    {
        feedTracker.markAsConsuming(link);
    }

    private void process(final Link link) throws Exception
    {
        try
        {
            consumeAction.consume(fetchFeedEntry(link));
        }
        catch (final Exception e)
        {
            feedTracker.revertConsuming(link);

            throw e;
        }
    }

    private ReadableRepresentation fetchFeedEntry(final Link link)
    {
        return resourceResolver.resolve(link);
    }

    private void markAsConsumed(final Link feedEntry) throws ExecutionException, RetryException
    {
        RETRY_BUILDER.build().call(new Callable<Void>()
        {
            @Override public Void call() throws Exception
            {
                feedTracker.markAsConsumed(feedEntry);
                return null;
            }
        });
    }

    private void notifyAllListeners(final Link consumedEntry)
    {
        for (final EntryConsumerListener_ listener : listeners)
        {
            listener.consumed(consumedEntry);
        }
    }
}
