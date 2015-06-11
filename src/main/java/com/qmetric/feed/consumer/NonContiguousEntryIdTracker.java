package com.qmetric.feed.consumer;

import com.google.common.base.Optional;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.Range;
import com.qmetric.feed.consumer.store.FeedTracker;

import static com.google.common.collect.ContiguousSet.create;
import static com.google.common.collect.DiscreteDomain.longs;

public class NonContiguousEntryIdTracker
{
    private final FeedTracker feedTracker;

    public NonContiguousEntryIdTracker(final FeedTracker feedTracker)
    {
        this.feedTracker = feedTracker;
    }

    public void trackMissingEntriesWithIdBetweenPreviousAndCurrentEntry(final SeenEntry currentEntry, final Optional<SeenEntry> previousEntry)
    {
        final boolean gapExistsBetweenEntryIds = previousEntry.isPresent() && currentEntry.id.asNumeric() > nextExpectedId(previousEntry);

        if (gapExistsBetweenEntryIds)
        {
            final ContiguousSet<Long> missingIds = create(Range.closedOpen(nextExpectedId(previousEntry), currentEntry.id.asNumeric()), longs());

            for (final long nextMissingId : missingIds)
            {
                feedTracker.track(new SeenEntry(EntryId.of(String.valueOf(nextMissingId)), currentEntry.dateTime));
            }
        }
    }

    private long nextExpectedId(final Optional<SeenEntry> previousEntry)
    {
        return previousEntry.get().id.next().asNumeric();
    }
}
