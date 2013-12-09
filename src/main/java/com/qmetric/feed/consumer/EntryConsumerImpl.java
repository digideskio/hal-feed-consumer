package com.qmetric.feed.consumer;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryerBuilder;
import com.qmetric.feed.consumer.store.AlreadyConsumingException;
import com.qmetric.feed.consumer.store.ConsumedStore;
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

    private final ConsumedStore consumedStore;

    private final ConsumeAction consumeAction;

    private final Collection<EntryConsumerListener> listeners;

    public EntryConsumerImpl(final ConsumedStore consumedStore, final ConsumeAction consumeAction, final Collection<EntryConsumerListener> listeners)
    {
        this.consumedStore = consumedStore;
        this.consumeAction = consumeAction;
        this.listeners = listeners;
    }

    @Override
    public void consume(ReadableRepresentation feedEntry) throws Exception
    {
        markAsConsuming(feedEntry);

        process(feedEntry);

        markAsConsumed(feedEntry);

        notifyAllListeners(feedEntry);
    }

    private void markAsConsuming(final ReadableRepresentation feedEntry) throws AlreadyConsumingException
    {
        consumedStore.markAsConsuming(feedEntry);
    }

    private void process(final ReadableRepresentation feedEntry) throws Exception
    {
        try
        {
            consumeAction.consume(feedEntry);
        }
        catch (final Throwable e)
        {
            consumedStore.revertConsuming(feedEntry);

            throw new Exception(e);
        }
    }

    private void markAsConsumed(final ReadableRepresentation feedEntry) throws ExecutionException, RetryException
    {
        RETRY_BUILDER.build().call(new Callable<Void>()
        {
            @Override public Void call() throws Exception
            {
                consumedStore.markAsConsumed(feedEntry);
                return null;
            }
        });
    }

    private void notifyAllListeners(final ReadableRepresentation consumedEntry)
    {
        for (final EntryConsumerListener listener : listeners)
        {
            listener.consumed(consumedEntry);
        }
    }
}
