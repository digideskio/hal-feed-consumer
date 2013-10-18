package com.qmetric.feed.consumer;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.qmetric.feed.consumer.store.ConsumedStore;
import com.theoryinpractise.halbuilder.DefaultRepresentationFactory;
import com.theoryinpractise.halbuilder.api.Link;
import com.theoryinpractise.halbuilder.api.ReadableRepresentation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Iterables.concat;

class UnconsumedFeedEntriesFinder
{
    private static final Logger log = LoggerFactory.getLogger(UnconsumedFeedEntriesFinder.class);

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss");

    private final ConsumedStore consumedStore;

    private final Optional<EarliestEntryLimit> earliestEntryLimit;

    private final RepresentationFactory representationFactory;

    private final FeedEndpointFactory feedEndpointFactory;

    UnconsumedFeedEntriesFinder(final FeedEndpointFactory feedEndpointFactory, final ConsumedStore consumedStore, final Optional<EarliestEntryLimit> earliestEntryLimit)
    {
        this.consumedStore = consumedStore;
        this.earliestEntryLimit = earliestEntryLimit;
        this.representationFactory = new DefaultRepresentationFactory();
        this.feedEndpointFactory = feedEndpointFactory;
    }

    List<ReadableRepresentation> findUnconsumed(final FeedEndpoint latestPageEndpoint)
    {
        return from(concat(ImmutableList.copyOf(new UnconsumedPageIterator(latestPageEndpoint)))) //
                .toList() //
                .reverse();
    }

    private class UnconsumedPageIterator implements Iterator<List<? extends ReadableRepresentation>>
    {
        private static final String NEXT_LINK_RELATION = "next";

        private static final String PUBLISHED = "_published";

        private Optional<ReadableRepresentation> currentPage;

        UnconsumedPageIterator(final FeedEndpoint latestPageEndpoint)
        {
            currentPage = Optional.of(representationFactory.readRepresentation(latestPageEndpoint.get()));
        }

        @Override public boolean hasNext()
        {
            return currentPage.isPresent();
        }

        @Override public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override public List<? extends ReadableRepresentation> next()
        {
            log.info("Reading feed-page {}", currentPageLink());

            final List<? extends ReadableRepresentation> allFromPage = currentPageEntries();

            final List<? extends ReadableRepresentation> unconsumedFromPage = unconsumedFrom(allFromPage);

            flipPage(allFromPage, unconsumedFromPage);

            return unconsumedFromPage;
        }

        private List<? extends ReadableRepresentation> currentPageEntries()
        {
            return currentPage.get().getResourcesByRel("entries");
        }

        private void flipPage(final List<? extends ReadableRepresentation> allFromPage, final List<? extends ReadableRepresentation> unconsumedFromPage)
        {
            log.debug("Found {}/{} unconsumed entries", allFromPage.size(), unconsumedFromPage.size());

            this.currentPage = allFromPage.size() == unconsumedFromPage.size() ? nextPage() : Optional.<ReadableRepresentation>absent();

            log.debug("Next page: {}", currentPage);
        }

        private String currentPageLink()
        {
            return currentPage.isPresent() ? currentPage.get().getResourceLink().getHref() : "CURRENT PAGE FIELD IS ABSENT";
        }

        private Optional<ReadableRepresentation> nextPage()
        {
            return nextLink(currentPage.get()).transform(new Function<Link, ReadableRepresentation>()
            {
                @Override public ReadableRepresentation apply(final Link link)
                {
                    return representationFactory.readRepresentation(feedEndpointFactory.create(link.getHref()).get());
                }
            });
        }

        private Optional<Link> nextLink(final ReadableRepresentation readableRepresentation)
        {
            return Optional.fromNullable(readableRepresentation.getLinkByRel(NEXT_LINK_RELATION));
        }

        private List<? extends ReadableRepresentation> unconsumedFrom(final List<? extends ReadableRepresentation> entries)
        {
            return from(entries).filter(new Predicate<ReadableRepresentation>()
            {
                public boolean apply(final ReadableRepresentation input)
                {
                    return hasConsumablePublishedDate(input) && consumedStore.notAlreadyConsumed(input.getResourceLink());
                }

                private boolean hasConsumablePublishedDate(final ReadableRepresentation entry)
                {
                    return !earliestEntryLimit.isPresent() || earliestEntryLimit.get().date.isBefore(publishedDate(entry));
                }

                private DateTime publishedDate(final ReadableRepresentation entry)
                {
                    return DATE_FORMATTER.parseDateTime((String) entry.getValue(PUBLISHED));
                }
            }).toList();
        }
    }
}
