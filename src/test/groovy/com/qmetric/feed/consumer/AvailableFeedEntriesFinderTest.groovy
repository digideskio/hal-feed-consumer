package com.qmetric.feed.consumer

import com.google.common.base.Optional
import com.google.common.io.Resources
import com.qmetric.feed.consumer.store.FeedTracker
import com.theoryinpractise.halbuilder.api.Link
import com.theoryinpractise.halbuilder.api.RepresentationFactory
import org.joda.time.DateTime
import spock.lang.Ignore
import spock.lang.Specification

class AvailableFeedEntriesFinderTest extends Specification
{

    def firstPageEndpoint = Mock(FeedEndpoint)

    def secondPageEndpoint = Mock(FeedEndpoint)

    def thirdPageEndpoint = Mock(FeedEndpoint)

    def tracker = Mock(FeedTracker)

    def feedEndpointFactory = Mock(FeedEndpointFactory)

    def finder = new AvailableFeedEntriesFinder(feedEndpointFactory, tracker, Optional.absent())

    def "should add all untracked entries"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithAllUnconsumed.json')
        tracker.isTracked(_ as Link) >>> [false, false]

        when:
        finder.findUnconsumed(firstPageEndpoint)

        then:
        1 * tracker.track(new Link(Mock(RepresentationFactory), 'self', 'http://feed/idOfOldestUnconsumed'))
        1 * tracker.track(new Link(Mock(RepresentationFactory), 'self', 'http://feed/idOfNewestUnconsumed'))
    }

    def "should add all untracked entries provided entries occurred after given earliest date restriction"()
    {
        given:
        def storeWithRestrictionOnEarliestDate = new AvailableFeedEntriesFinder(feedEndpointFactory, tracker, Optional.of(new EarliestEntryLimit(new DateTime(2013, 5, 23, 0, 0, 1))))
        firstPageEndpoint.get() >> reader('/feedWithAllUnconsumed.json')
        tracker.isTracked(_ as Link) >>> [false, false]

        when:
        storeWithRestrictionOnEarliestDate.findUnconsumed(firstPageEndpoint)

        then:
        1 * tracker.track(new Link(Mock(RepresentationFactory), 'self', 'http://feed/idOfNewestUnconsumed'))
    }

    def "should return only untracked entries provided feed contains both tracked and untracked"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithSomeUnconsumed.json')
        tracker.isTracked(_ as Link) >>> [false, false, false, true]

        when:
        finder.findUnconsumed(firstPageEndpoint)

        then:
        3 * tracker.track(_ as Link)
    }

    def "should walk to next feed page if all entries in the current page are untracked"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithAllUnconsumedAndNextLink.json')
        feedEndpointFactory.create(_ as String) >> secondPageEndpoint
        secondPageEndpoint.get() >> reader('/feedWithSomeUnconsumed.json')
        tracker.isTracked(_ as Link) >>> [false, false, false, true]

        when:
        finder.findUnconsumed(firstPageEndpoint)

        then:
        1 * tracker.track(new Link(Mock(RepresentationFactory), 'self', 'http://feed/idOfNewUnconsumed'))
        1 * tracker.track(new Link(Mock(RepresentationFactory), 'self', 'http://feed/idOfNewerUnconsumed'))
        1 * tracker.track(new Link(Mock(RepresentationFactory), 'self', 'http://feed/idOfNewestUnconsumed'))
    }

    def "should walk all the pages until a tracked entry is found"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithAllUnconsumedAndNextLink.json')
        feedEndpointFactory.create(_ as String) >>> [secondPageEndpoint, thirdPageEndpoint]
        secondPageEndpoint.get() >> reader('/anotherFeedWithAllUnconsumedAndNextLink.json')
        thirdPageEndpoint.get() >> reader('/feedWithSomeUnconsumed.json')
        tracker.isTracked(_ as Link) >>> [false, false, false, false, false, true]

        when:
        finder.findUnconsumed(firstPageEndpoint)

        then:
        5 * tracker.track(_ as Link)
    }

    def "should return none when tracker already contains all entries"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithAllConsumed.json')
        tracker.isTracked(_ as Link) >> true

        when:
        finder.findUnconsumed(firstPageEndpoint)

        then:
        0 * tracker.track(_ as Link)
    }

    private static reader(String resourcePath)
    {
        new InputStreamReader(Resources.getResourceAsStream(resourcePath))
    }
}
