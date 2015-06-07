package com.qmetric.feed.consumer;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.qmetric.feed.consumer.store.FeedTracker;
import com.qmetric.hal.reader.HalReader;
import com.qmetric.hal.reader.HalResource;
import com.theoryinpractise.halbuilder.api.Link;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.FluentIterable.from;

class UnconsumedPageIterator implements Iterator<List<EntryId>>
{
    private static final Logger log = LoggerFactory.getLogger(UnconsumedPageIterator.class);

    private static final String NEXT_LINK_RELATION = "next";

    private static final String PUBLISHED = "_published";

    private static final String ENTRY_ID = "_id";

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss");

    private final HalReader halReader;

    private final FeedTracker feedTracker;

    private final FeedEndpointFactory feedEndpointFactory;

    private final Optional<EarliestEntryLimit> earliestEntryLimit;

    private Optional<HalResource> currentPage;

    UnconsumedPageIterator(final HalReader halReader, final FeedEndpoint endpoint, FeedTracker feedTracker, FeedEndpointFactory feedEndpointFactory, Optional<EarliestEntryLimit> earliestEntryLimit)
    {
        this.halReader = halReader;
        this.feedTracker = feedTracker;
        this.feedEndpointFactory = feedEndpointFactory;
        this.earliestEntryLimit = earliestEntryLimit;
        currentPage = Optional.of(halReader.read(endpoint.get()));
    }

    @Override public boolean hasNext()
    {
        return currentPage.isPresent();
    }

    @Override public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override public List<EntryId> next()
    {
        log.info("Reading feed-page {}", currentPage.get().getResourceLink().get().getHref());

        final List<HalResource> allFromPage = currentPageEntries();

        final List<HalResource> newEntries = newEntries(allFromPage);

        flipToNextPage(allFromPage, newEntries);

        return from(newEntries).transform(new Function<HalResource, EntryId>()
        {
            @Override public EntryId apply(final HalResource input)
            {
                return idOf(input);
            }
        }).toList();
    }

    private List<HalResource> currentPageEntries()
    {
        return currentPage.get().getResourcesByRel("entries");
    }

    private EntryId idOf(final HalResource entry)
    {
        return EntryId.of(entry.getValueAsString(ENTRY_ID).get());
    }

    private void flipToNextPage(final List<HalResource> allFromPage, final List<HalResource> unconsumedFromPage)
    {
        log.debug("Found {} unconsumed entries out of {}", unconsumedFromPage.size(), allFromPage.size());

        this.currentPage = allFromPage.size() == unconsumedFromPage.size() ? getNextPage() : Optional.<HalResource>absent();

        log.debug("Next page: {}", currentPage);
    }

    private Optional<HalResource> getNextPage()
    {
        return nextLink(currentPage.get()).transform(new Function<Link, HalResource>()
        {
            @Override public HalResource apply(final Link link)
            {
                return halReader.read(feedEndpointFactory.create(link.getHref()).get());
            }
        });
    }

    private Optional<Link> nextLink(final HalResource entry)
    {
        return entry.getLinkByRel(NEXT_LINK_RELATION);
    }

    private List<HalResource> newEntries(final List<HalResource> entries)
    {
        return from(entries).filter(new Predicate<HalResource>()
        {
            @Override
            public boolean apply(final HalResource input)
            {
                return hasConsumablePublishedDate(input) && isNotTracked(input);
            }

            private boolean isNotTracked(final HalResource entry)
            {
                return !feedTracker.isTracked(idOf(entry));
            }

            private boolean hasConsumablePublishedDate(final HalResource entry)
            {
                return !earliestEntryLimit.isPresent() || earliestEntryLimit.get().date.isBefore(publishedDate(entry));
            }

            private DateTime publishedDate(final HalResource entry)
            {
                return DATE_FORMATTER.parseDateTime(entry.getValueAsString(PUBLISHED).get());
            }
        }).toList();
    }
}