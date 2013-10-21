package com.qmetric.feed.consumer

import com.google.common.base.Optional
import com.google.common.io.Resources
import com.qmetric.feed.consumer.store.FeedTracker
import com.theoryinpractise.halbuilder.api.ReadableRepresentation
import org.joda.time.DateTime
import spock.lang.Ignore
import spock.lang.Specification

class UnconsumedFeedEntriesFinderTest extends Specification
{

    def firstPageEndpoint = Mock(FeedEndpoint)

    def secondPageEndpoint = Mock(FeedEndpoint)

    def thirdPageEndpoint = Mock(FeedEndpoint)

    def consumedStore = Mock(FeedTracker)

    def feedEndpointFactory = Mock(FeedEndpointFactory)

    def store = new UnconsumedFeedEntriesFinder(feedEndpointFactory, consumedStore, Optional.absent())

    def "should return all entries provided feed contains all unconsumed entries"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithAllUnconsumed.json')
        consumedStore.notAlreadyConsumed(_) >>> [true, true]

        when:
        List<ReadableRepresentation> unprocessedList = store.findUnconsumed(firstPageEndpoint)

        then:
        unprocessedList.collect { it.getValue("_id") } == ['idOfOldestUnconsumed', 'idOfNewestUnconsumed']
    }

    def "should return all unconsumed entries provided entries occurred after given earliest date restriction"()
    {
        given:
        def storeWithRestrictionOnEarliestDate = new UnconsumedFeedEntriesFinder(feedEndpointFactory, consumedStore, Optional.of(new EarliestEntryLimit(new DateTime(2013, 5, 23, 0, 0, 1))))
        firstPageEndpoint.get() >> reader('/feedWithAllUnconsumed.json')
        consumedStore.notAlreadyConsumed(_) >>> [true, true]

        when:
        List<ReadableRepresentation> unprocessedList = storeWithRestrictionOnEarliestDate.findUnconsumed(firstPageEndpoint)

        then:
        unprocessedList.collect { it.getValue("_id") } == ['idOfNewestUnconsumed']
    }

    def "should return only unconsumed entries provided feed contains some unconsumed entries"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithSomeUnconsumed.json')
        consumedStore.notAlreadyConsumed(_) >>> [true, true, true, false]

        when:
        List<ReadableRepresentation> unprocessedList = store.findUnconsumed(firstPageEndpoint)

        then:
        unprocessedList.size() == 3
    }

    def "should paginate to next feed if all entries in the feed are unconsumed"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithAllUnconsumedAndNextLink.json')
        feedEndpointFactory.create(_ as String) >> secondPageEndpoint
        secondPageEndpoint.get() >> reader('/feedWithSomeUnconsumed.json')
        consumedStore.notAlreadyConsumed(_) >>> [true, true, true, false]


        when:
        List<ReadableRepresentation> unprocessedList = store.findUnconsumed(firstPageEndpoint)

        then:
        unprocessedList.collect { it.getValue("_id") } == ['idOfNewUnconsumed', 'idOfNewerUnconsumed', 'idOfNewestUnconsumed']
    }

    def "should paginate to multiple pages"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithAllUnconsumedAndNextLink.json')
        feedEndpointFactory.create(_ as String) >>> [secondPageEndpoint, thirdPageEndpoint]
        secondPageEndpoint.get() >> reader('/anotherFeedWithAllUnconsumedAndNextLink.json')
        thirdPageEndpoint.get() >> reader('/feedWithSomeUnconsumed.json')

        consumedStore.notAlreadyConsumed(_) >>> [true, true, true, true, true, false]

        when:
        List<ReadableRepresentation> unprocessedList = store.findUnconsumed(firstPageEndpoint)

        then:
        unprocessedList.size() == 5
    }

    @Ignore("Is it supposed to walk through all the pages skipping the consumed ones?")
    def "should paginate to multiple pages skipping consumed entries"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithAllUnconsumedAndNextLink.json')
        feedEndpointFactory.create(_ as String) >>> [secondPageEndpoint, thirdPageEndpoint]
        secondPageEndpoint.get() >> reader('/anotherFeedWithAllUnconsumedAndNextLink.json')
        thirdPageEndpoint.get() >> reader('/feedWithSomeUnconsumed.json')

        consumedStore.notAlreadyConsumed(_) >>> [false, true, true, true, true, false]

        when:
        List<ReadableRepresentation> unprocessedList = store.findUnconsumed(firstPageEndpoint)

        then:
        unprocessedList.size() == 4
    }

    def "should return none when feed has all consumed"()
    {
        given:
        firstPageEndpoint.get() >> reader('/feedWithAllConsumed.json')
        consumedStore.notAlreadyConsumed(_) >>> [false]

        when:
        List<ReadableRepresentation> unprocessedList = store.findUnconsumed(firstPageEndpoint)

        then:
        unprocessedList.isEmpty()
    }

    private static reader(String resourcePath)
    {
        new InputStreamReader(Resources.getResourceAsStream(resourcePath))
    }
}
