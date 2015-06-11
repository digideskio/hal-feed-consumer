package com.qmetric.feed.consumer;

import com.google.common.base.Optional;
import com.qmetric.feed.consumer.store.FeedTracker;
import com.qmetric.hal.reader.HalReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AvailableFeedEntriesTracker
{
    private static final Logger LOG = LoggerFactory.getLogger(AvailableFeedEntriesTracker.class);

    private final FeedEndpoint endpoint;

    private final FeedTracker feedTracker;

    private final HalReader halReader;

    private final FeedEndpointFactory feedEndpointFactory;

    private final NonContiguousEntryIdTracker nonContiguousEntryIdTracker;

    private final PageOfSeenEntriesFactory pageOfSeenEntriesFactory;

    AvailableFeedEntriesTracker(final FeedEndpoint endpoint, final FeedEndpointFactory feedEndpointFactory, final FeedTracker feedTracker,
                                final HalReader halReader, final NonContiguousEntryIdTracker nonContiguousEntryIdTracker, final PageOfSeenEntriesFactory pageOfSeenEntriesFactory)
    {
        this.endpoint = endpoint;
        this.feedTracker = feedTracker;
        this.halReader = halReader;
        this.feedEndpointFactory = feedEndpointFactory;
        this.nonContiguousEntryIdTracker = nonContiguousEntryIdTracker;
        this.pageOfSeenEntriesFactory = pageOfSeenEntriesFactory;
    }

    public void trackNewEntries()
    {
        final UntrackedPageIterator pagesWithUntrackedEntries = new UntrackedPageIterator(halReader, endpoint, feedEndpointFactory, pageOfSeenEntriesFactory);

        Optional<SeenEntry> previousEntry = Optional.absent();

        while (pagesWithUntrackedEntries.hasNext())
        {
            for (final SeenEntry entry : pagesWithUntrackedEntries.next().all())
            {
                nonContiguousEntryIdTracker.trackMissingEntriesWithIdBetweenPreviousAndCurrentEntry(entry, previousEntry);

                if (entry.notAlreadyTracked())
                {
                    LOG.info("found new entry {}", entry);

                    feedTracker.track(entry);
                }

                previousEntry = Optional.of(entry);
            }
        }
    }
}
