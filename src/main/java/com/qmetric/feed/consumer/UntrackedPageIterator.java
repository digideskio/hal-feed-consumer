package com.qmetric.feed.consumer;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.qmetric.hal.reader.HalReader;
import com.qmetric.hal.reader.HalResource;
import com.theoryinpractise.halbuilder.api.Link;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

class UntrackedPageIterator implements Iterator<PageOfSeenEntries>
{
    private static final Logger LOG = LoggerFactory.getLogger(UntrackedPageIterator.class);

    private static final String NEXT_LINK_RELATION = "next";

    private static final String PREVIOUS_LINK_RELATION = "previous";

    private static final String ENTRIES = "entries";

    private final HalReader halReader;

    private final PageOfSeenEntriesFactory seenEntriesFactory;

    private final FeedEndpointFactory feedEndpointFactory;

    private Optional<HalResource> currentPage;

    UntrackedPageIterator(final HalReader halReader, final FeedEndpoint endpoint, final FeedEndpointFactory feedEndpointFactory,
                          final PageOfSeenEntriesFactory seenEntriesFactory)
    {
        this.halReader = halReader;
        this.seenEntriesFactory = seenEntriesFactory;
        this.feedEndpointFactory = feedEndpointFactory;
        currentPage = Optional.of(navigateToEarliestPageToTrack(halReader.read(endpoint.get().get())));
    }

    @Override public boolean hasNext()
    {
        return currentPage.isPresent();
    }

    @Override public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override public PageOfSeenEntries next()
    {
        LOG.debug("Reading feed-page {}", currentPage.get().getResourceLink().get().getHref());

        final PageOfSeenEntries allEntriesFromPage = seenEntriesFactory.create(currentPageEntries(currentPage.get())).reverse();

        final List<SeenEntry> untrackedEntries = allEntriesFromPage.untrackedEntries();

        LOG.debug("Found {} untracked entries out of {}", untrackedEntries.size(), allEntriesFromPage.all().size());

        flipToPreviousPage();

        return allEntriesFromPage;
    }

    private HalResource navigateToEarliestPageToTrack(final HalResource latestPage)
    {
        Optional<HalResource> currentPage = Optional.of(latestPage);
        HalResource earliestPage = latestPage;

        while (currentPage.isPresent())
        {
            LOG.debug("Finding earliest page to track: {}", currentPage);

            final PageOfSeenEntries allEntriesFromPage = seenEntriesFactory.create(currentPageEntries(currentPage.get()));
            final List<SeenEntry> untrackedEntries = allEntriesFromPage.untrackedEntries();

            earliestPage = currentPage.get();
            currentPage = untrackedEntries.isEmpty() ? Optional.<HalResource>absent() : loadAdjacentPage(currentPage.get(), NEXT_LINK_RELATION);
        }

        LOG.debug("Found earliest page to track: {}", earliestPage);

        return earliestPage;
    }

    private List<HalResource> currentPageEntries(final HalResource currentPage)
    {
        return currentPage.getResourcesByRel(ENTRIES);
    }

    private void flipToPreviousPage()
    {
        this.currentPage = loadAdjacentPage(currentPage.get(), PREVIOUS_LINK_RELATION);

        LOG.debug("Previous page: {}", currentPage);
    }

    private Optional<HalResource> loadAdjacentPage(final HalResource currentPage, final String linkRel)
    {
        return linkToAdjacentPage(currentPage, linkRel).transform(new Function<Link, HalResource>()
        {
            @Override public HalResource apply(final Link link)
            {
                LOG.info("Navigating to page: {}", link.getHref());
                return halReader.read(feedEndpointFactory.create(link.getHref()).get().get());
            }
        });
    }

    private Optional<Link> linkToAdjacentPage(final HalResource entry, final String linkRel)
    {
        return entry.getLinkByRel(linkRel);
    }
}