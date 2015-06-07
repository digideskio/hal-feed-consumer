package com.qmetric.feed.consumer;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.qmetric.feed.consumer.store.FeedTracker;
import com.qmetric.hal.reader.HalReader;

import java.util.List;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Iterables.concat;

class AvailableFeedEntriesFinder
{
    private final FeedEndpoint endpoint;

    private final FeedTracker feedTracker;

    private final Optional<EarliestEntryLimit> earliestEntryLimit;

    private final HalReader halReader;

    private final FeedEndpointFactory feedEndpointFactory;

    AvailableFeedEntriesFinder(final FeedEndpoint endpoint, final FeedEndpointFactory feedEndpointFactory, final FeedTracker feedTracker,
                               final Optional<EarliestEntryLimit> earliestEntryLimit, final HalReader halReader)
    {
        this.endpoint = endpoint;
        this.feedTracker = feedTracker;
        this.earliestEntryLimit = earliestEntryLimit;
        this.halReader = halReader;
        this.feedEndpointFactory = feedEndpointFactory;
    }

    public void trackNewEntries()
    {
        trackAll(newEntries());
    }

    private void trackAll(final List<EntryId> newEntries)
    {
        for (final EntryId entry : newEntries)
        {
            feedTracker.track(entry);
        }
    }

    private List<EntryId> newEntries()
    {
        final ReverseUnconsumedPageIterator iterator = new ReverseUnconsumedPageIterator(halReader, endpoint, feedTracker, feedEndpointFactory, earliestEntryLimit);

        Optional<EntryId> latestConsumed = iterator.getLatestEntryConsumed();

        EntryId nextExpectedEntry = EntryId.of(String.valueOf(latestConsumed.get().asNumeric() + 1));

        while (iterator.hasNext())
        {
            for (final EntryId entryId: iterator.next())
            {
                if (!entryId.equals(nextExpectedEntry))
                {
                    // track each missing entry
                }

                // track current entry

                nextExpectedEntry = EntryId.of(String.valueOf(nextExpectedEntry.asNumeric() + 1));
            }
        }

        return from(concat(ImmutableList.copyOf(iterator))) //
                .toList() //
                .reverse();
    }
}
