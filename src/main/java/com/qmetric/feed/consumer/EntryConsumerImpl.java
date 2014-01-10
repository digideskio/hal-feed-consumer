package com.qmetric.feed.consumer;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryerBuilder;
import com.qmetric.feed.consumer.store.AlreadyConsumingException;
import com.qmetric.feed.consumer.store.FeedTracker;
import com.theoryinpractise.halbuilder.api.ReadableRepresentation;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static com.github.rholder.retry.StopStrategies.stopAfterAttempt;
import static com.github.rholder.retry.WaitStrategies.fixedWait;
import static java.util.concurrent.TimeUnit.SECONDS;

public class EntryConsumerImpl implements EntryConsumer
{
    private static final RetryerBuilder<Void> RETRY_BUILDER = RetryerBuilder.<Void>newBuilder() //
            .retryIfException() //
            .withWaitStrategy(fixedWait(1, SECONDS)) //
            .withStopStrategy(stopAfterAttempt(60));

    private final FeedTracker feedTracker;

    private final ConsumeAction consumeAction;

    private final ResourceResolver resourceResolver;

    private final Collection<EntryConsumerListener> listeners;

    public EntryConsumerImpl(final FeedTracker feedTracker, final ConsumeAction consumeAction, final ResourceResolver resourceResolver,
                             final Collection<EntryConsumerListener> listeners)
    {
        this.feedTracker = feedTracker;
        this.consumeAction = consumeAction;
        this.resourceResolver = resourceResolver;
        this.listeners = listeners;
    }

    @Override
    public void consume(EntryId id) throws Exception
    {
        markAsConsuming(id);

        process(id);

        markAsConsumed(id);

        notifyAllListeners(id);
    }

    private void markAsConsuming(final EntryId id) throws AlreadyConsumingException
    {
        feedTracker.markAsConsuming(id);
    }

    private void process(final EntryId id) throws Exception
    {
        try
        {
            consumeAction.consume(fetchFeedEntry(id));
        }
        catch (final Exception e)
        {
            feedTracker.fail(id);
            throw e;
        }
    }

    private ReadableRepresentation fetchFeedEntry(final EntryId id)
    {
        return resourceResolver.resolve(id);
    }

    private void markAsConsumed(final EntryId id) throws ExecutionException, RetryException
    {
        RETRY_BUILDER.build().call(new Callable<Void>()
        {
            @Override public Void call() throws Exception
            {
                feedTracker.markAsConsumed(id);
                return null;
            }
        });
    }

    private void notifyAllListeners(final EntryId id)
    {
        for (final EntryConsumerListener listener : listeners)
        {
            listener.consumed(id);
        }
    }
}
