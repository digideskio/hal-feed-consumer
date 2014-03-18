package com.qmetric.feed.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Optional
import com.google.common.io.Resources
import com.qmetric.feed.consumer.store.FeedTracker
import com.qmetric.hal.reader.HalReader
import org.joda.time.DateTime
import spock.lang.Specification

class AvailableFeedEntriesFinderTest extends Specification {

    def feedEndpoint = Mock(FeedEndpoint)

    def secondPageEndpoint = Mock(FeedEndpoint)

    def thirdPageEndpoint = Mock(FeedEndpoint)

    def tracker = Mock(FeedTracker)

    def feedEndpointFactory = Mock(FeedEndpointFactory)

    def halReader = new HalReader(new ObjectMapper())

    def finder = new AvailableFeedEntriesFinder(feedEndpoint, feedEndpointFactory, tracker, Optional.absent(), halReader)

    def "should add all untracked entries"()
    {
        given:
        feedEndpoint.get() >> reader('/feedWithAllUnconsumed.json')
        tracker.isTracked(_ as EntryId) >>> [false, false]

        when:
        finder.trackNewEntries()

        then:
        1 * tracker.track(EntryId.of('idOfOldestUnconsumed'))
        1 * tracker.track(EntryId.of('idOfNewestUnconsumed'))
    }

    def "should add all untracked entries provided entries occurred after given earliest date restriction"()
    {
        given:
        def storeWithRestrictionOnEarliestDate = new AvailableFeedEntriesFinder(feedEndpoint, feedEndpointFactory, tracker, earliestEntryDate(), halReader)
        feedEndpoint.get() >> reader('/feedWithAllUnconsumed.json')
        tracker.isTracked(_ as EntryId) >>> [false, false]

        when:
        storeWithRestrictionOnEarliestDate.trackNewEntries()

        then:
        1 * tracker.track(EntryId.of('idOfNewestUnconsumed'))
    }

    def "should return only untracked entries provided feed contains both tracked and untracked"()
    {
        given:
        feedEndpoint.get() >> reader('/feedWithSomeUnconsumed.json')
        tracker.isTracked(_ as EntryId) >>> [false, false, false, true]

        when:
        finder.trackNewEntries()

        then:
        3 * tracker.track(_ as EntryId)
    }

    def "should walk to next feed page if all entries in the current page are untracked"()
    {
        given:
        feedEndpoint.get() >> reader('/feedWithAllUnconsumedAndNextLink.json')
        feedEndpointFactory.create(_ as String) >> secondPageEndpoint
        secondPageEndpoint.get() >> reader('/feedWithSomeUnconsumed.json')
        tracker.isTracked(_ as EntryId) >>> [false, false, false, true]

        when:
        finder.trackNewEntries()

        then:
        1 * tracker.track(EntryId.of('idOfNewUnconsumed'))
        1 * tracker.track(EntryId.of('idOfNewerUnconsumed'))
        1 * tracker.track(EntryId.of('idOfNewestUnconsumed'))
    }

    def "should walk all the pages until a tracked entry is found"()
    {
        given:
        feedEndpoint.get() >> reader('/feedWithAllUnconsumedAndNextLink.json')
        feedEndpointFactory.create(_ as String) >>> [secondPageEndpoint, thirdPageEndpoint]
        secondPageEndpoint.get() >> reader('/anotherFeedWithAllUnconsumedAndNextLink.json')
        thirdPageEndpoint.get() >> reader('/feedWithSomeUnconsumed.json')
        tracker.isTracked(_ as EntryId) >>> [false, false, false, false, false, true]

        when:
        finder.trackNewEntries()

        then:
        5 * tracker.track(_ as EntryId)
    }

    def "should return none when tracker already contains all entries"()
    {
        given:
        feedEndpoint.get() >> reader('/feedWithAllConsumed.json')
        tracker.isTracked(_ as EntryId) >> true

        when:
        finder.trackNewEntries()

        then:
        0 * tracker.track(_ as EntryId)
    }

    private static reader(String resourcePath)
    {
        new InputStreamReader(Resources.getResourceAsStream(resourcePath))
    }

    private static Optional<EarliestEntryLimit> earliestEntryDate()
    {
        Optional.of(new EarliestEntryLimit(new DateTime(2013, 5, 23, 0, 0, 1)))
    }
}
