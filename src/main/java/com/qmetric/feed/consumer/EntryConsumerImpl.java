package com.qmetric.feed.consumer;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryerBuilder;
import com.google.common.base.Optional;
import com.qmetric.feed.consumer.store.AlreadyConsumingException;
import com.qmetric.feed.consumer.store.FeedTracker;
import com.qmetric.hal.reader.HalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static com.github.rholder.retry.StopStrategies.stopAfterAttempt;
import static com.github.rholder.retry.WaitStrategies.fixedWait;
import static com.qmetric.feed.consumer.Result.*;
import static java.util.concurrent.TimeUnit.SECONDS;

public class EntryConsumerImpl implements EntryConsumer
{
    private static final Logger LOG = LoggerFactory.getLogger(EntryConsumerImpl.class);

    private static final RetryerBuilder<Void> RETRY_BUILDER = RetryerBuilder.<Void>newBuilder() //
            .retryIfException() //
            .withWaitStrategy(fixedWait(1, SECONDS)) //
            .withStopStrategy(stopAfterAttempt(60));

    private final FeedTracker feedTracker;

    private final ConsumeAction consumeAction;

    private final ResourceResolver resourceResolver;

    private final Collection<EntryConsumerListener> listeners;

    private final Optional<Integer> maxRetries;

    private final Interval timeoutForMissingEntries;

    private final DateTimeSource dateTimeSource;

    public EntryConsumerImpl(final FeedTracker feedTracker, final ConsumeAction consumeAction, final ResourceResolver resourceResolver,
                             final Collection<EntryConsumerListener> listeners, final Optional<Integer> maxRetries, final Interval timeoutForMissingEntries,
                             final DateTimeSource dateTimeSource)
    {
        this.feedTracker = feedTracker;
        this.consumeAction = consumeAction;
        this.resourceResolver = resourceResolver;
        this.listeners = listeners;
        this.maxRetries = maxRetries;
        this.timeoutForMissingEntries = timeoutForMissingEntries;
        this.dateTimeSource = dateTimeSource;
    }

    @Override
    public boolean consume(final TrackedEntry trackedEntry) throws Exception
    {
        markAsConsuming(trackedEntry);

        final boolean success = process(trackedEntry);

        if (success)
        {
            markAsConsumed(trackedEntry);

            notifyAllListeners(trackedEntry);
        }

        return success;
    }

    private void markAsConsuming(final TrackedEntry trackedEntry) throws AlreadyConsumingException
    {
        feedTracker.markAsConsuming(trackedEntry.id);
    }

    private boolean process(final TrackedEntry trackedEntry) throws Exception
    {
        try
        {
            final Optional<FeedEntry> feedEntry = fetchFeedEntry(trackedEntry);

            if (feedEntry.isPresent())
            {
                final Result result = consumeAction.consume(feedEntry.get());
                if (result.failure())
                {
                    feedTracker.fail(trackedEntry, result.state == State.RETRY_UNSUCCESSFUL);
                    return false;
                }
                else
                {
                    return true;
                }
            }
            else
            {
                LOG.info("tracked feed entry {} not found in feed - ignoring as tracker assumes some entries exist when non-contiguous feed entry ids", trackedEntry);
                feedTracker.fail(trackedEntry, scheduleRetryToConsumeMissingEntry(trackedEntry));
                return false;
            }
        }
        catch (final Throwable e)
        {
            fail(trackedEntry);
            throw new Exception(e);
        }
    }

    private boolean scheduleRetryToConsumeMissingEntry(final TrackedEntry trackedEntry)
    {
        return trackedEntry.created != null && dateTimeSource.now().isBefore(trackedEntry.created.plus(timeoutForMissingEntries.asMillis()));
    }

    private void fail(final TrackedEntry trackedEntry)
    {
        final boolean scheduleRetry = !maxRetries.isPresent() || maxRetries.get() > trackedEntry.retries;

        feedTracker.fail(trackedEntry, scheduleRetry);
    }

    private Optional<FeedEntry> fetchFeedEntry(final TrackedEntry trackedEntry)
    {
        final Optional<HalResource> resolved = resourceResolver.resolve(trackedEntry.id);
        return resolved.isPresent() ? Optional.of(new FeedEntry(resolved.get(), trackedEntry.retries)) : Optional.<FeedEntry>absent();
    }

    private void markAsConsumed(final TrackedEntry trackedEntry) throws ExecutionException, RetryException
    {
        RETRY_BUILDER.build().call(new Callable<Void>()
        {
            @Override public Void call() throws Exception
            {
                feedTracker.markAsConsumed(trackedEntry.id);
                return null;
            }
        });
    }

    private void notifyAllListeners(final TrackedEntry trackingEntry)
    {
        for (final EntryConsumerListener listener : listeners)
        {
            listener.consumed(trackingEntry.id);
        }
    }
}
