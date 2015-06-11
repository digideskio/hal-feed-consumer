package com.qmetric.feed.consumer;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.qmetric.feed.consumer.store.FeedTracker;
import com.qmetric.hal.reader.HalResource;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

import static com.google.common.collect.FluentIterable.from;

public class PageOfSeenEntriesFactory
{
    private static final String PUBLISHED = "_published";

    private static final String ENTRY_ID = "_id";

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss");

    private final FeedTracker feedTracker;

    private final Optional<EarliestEntryLimit> earliestEntryLimit;

    public PageOfSeenEntriesFactory(final FeedTracker feedTracker, final Optional<EarliestEntryLimit> earliestEntryLimit)
    {
        this.feedTracker = feedTracker;
        this.earliestEntryLimit = earliestEntryLimit;
    }

    public PageOfSeenEntries create(final List<HalResource> entries)
    {
        return new PageOfSeenEntries(from(entries).transform(new Function<HalResource, SeenEntry>()
        {
            @Override public SeenEntry apply(final HalResource input)
            {
                return asSeenEntry(input);
            }

            private boolean isNotTracked(final EntryId entryId)
            {
                return !feedTracker.isTracked(entryId);
            }

            private boolean hasConsumablePublishedDate(final DateTime publishedDate)
            {
                return !earliestEntryLimit.isPresent() || earliestEntryLimit.get().date.isBefore(publishedDate);
            }

            private SeenEntry asSeenEntry(final HalResource entry)
            {
                final EntryId id = EntryId.of(entry.getValueAsString(ENTRY_ID).get());

                final DateTime publishedDate = DATE_FORMATTER.parseDateTime(entry.getValueAsString(PUBLISHED).get());

                return new SeenEntry(id, publishedDate, !(hasConsumablePublishedDate(publishedDate) && isNotTracked(id)));
            }
        }).toList());
    }
}
