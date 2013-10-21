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

    def "should add all entries untracked entries"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithAllUnconsumed.json')
        tracker.isTracked(_ as Link) >>> [true, true]

        when:
        finder.findUnconsumed(firstPageEndpoint)

        then:
        1 * tracker.track(new Link(Mock(RepresentationFactory), 'self', 'http://feed/idOfOldestUnconsumed'))
        1 * tracker.track(new Link(Mock(RepresentationFactory), 'self', 'http://feed/idOfNewestUnconsumed'))
    }

    def "should return all unconsumed entries provided entries occurred after given earliest date restriction"()
    {
        given:
        def storeWithRestrictionOnEarliestDate = new AvailableFeedEntriesFinder(feedEndpointFactory, tracker, Optional.of(new EarliestEntryLimit(new DateTime(2013, 5, 23, 0, 0, 1))))
        firstPageEndpoint.get() >> reader('/feedWithAllUnconsumed.json')
        tracker.isTracked(_ as Link) >>> [true, true]

        when:
        storeWithRestrictionOnEarliestDate.findUnconsumed(firstPageEndpoint)

        then:
        1 * tracker.track(new Link(Mock(RepresentationFactory), 'self', 'http://feed/idOfNewestUnconsumed'))
    }

    def "should return only unconsumed entries provided feed contains some unconsumed entries"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithSomeUnconsumed.json')
        tracker.isTracked(_ as Link) >>> [true, true, true, false]

        when:
        finder.findUnconsumed(firstPageEndpoint)

        then:
        3 * tracker.track(_ as Link)
    }

    def "should paginate to next feed if all entries in the feed are unconsumed"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithAllUnconsumedAndNextLink.json')
        feedEndpointFactory.create(_ as String) >> secondPageEndpoint
        secondPageEndpoint.get() >> reader('/feedWithSomeUnconsumed.json')
        tracker.isTracked(_ as Link) >>> [true, true, true, false]

        when:
        finder.findUnconsumed(firstPageEndpoint)

        then:
        1 * tracker.track(new Link(Mock(RepresentationFactory), 'self', 'http://feed/idOfNewUnconsumed'))
        1 * tracker.track(new Link(Mock(RepresentationFactory), 'self', 'http://feed/idOfNewerUnconsumed'))
        1 * tracker.track(new Link(Mock(RepresentationFactory), 'self', 'http://feed/idOfNewestUnconsumed'))
    }

    def "should paginate to multiple pages"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithAllUnconsumedAndNextLink.json')
        feedEndpointFactory.create(_ as String) >>> [secondPageEndpoint, thirdPageEndpoint]
        secondPageEndpoint.get() >> reader('/anotherFeedWithAllUnconsumedAndNextLink.json')
        thirdPageEndpoint.get() >> reader('/feedWithSomeUnconsumed.json')
        tracker.isTracked(_ as Link) >>> [true, true, true, true, true, false]

        when:
        finder.findUnconsumed(firstPageEndpoint)

        then:
        5 * tracker.track(_ as Link)
    }

    @Ignore("Is it supposed to walk through all the pages skipping the consumed ones?") def "should paginate to multiple pages skipping consumed entries"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithAllUnconsumedAndNextLink.json')
        feedEndpointFactory.create(_ as String) >>> [secondPageEndpoint, thirdPageEndpoint]
        secondPageEndpoint.get() >> reader('/anotherFeedWithAllUnconsumedAndNextLink.json')
        thirdPageEndpoint.get() >> reader('/feedWithSomeUnconsumed.json')
        tracker.isTracked(_ as Link) >>> [false, true, true, true, true, false]

        when:
        finder.findUnconsumed(firstPageEndpoint)

        then:
        4 * tracker.track(_ as Link)
    }

    def "should return none when feed has all consumed"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithAllConsumed.json')
        tracker.isTracked(_ as Link) >>> [false]

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
