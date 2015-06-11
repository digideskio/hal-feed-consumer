package com.qmetric.feed.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Optional
import com.google.common.io.Resources
import com.qmetric.feed.consumer.store.FeedTracker
import com.qmetric.hal.reader.HalReader
import org.joda.time.DateTime
import spock.lang.Specification

import static com.google.common.base.Optional.absent

class AvailableFeedEntriesTrackerTest extends Specification {

    static final TRACKED = true

    static final UNTRACKED = false

    final feedEndpoint = Mock(FeedEndpoint)

    final secondPageEndpoint = Mock(FeedEndpoint)

    final thirdPageEndpoint = Mock(FeedEndpoint)

    final tracker = Mock(FeedTracker)

    final feedEndpointFactory = Mock(FeedEndpointFactory)

    final nonContiguousEntryTracker = Mock(NonContiguousEntryIdTracker)

    final halReader = new HalReader(new ObjectMapper())

    final availableEntriesTracker = new AvailableFeedEntriesTracker(feedEndpoint, feedEndpointFactory, tracker, halReader, nonContiguousEntryTracker, new PageOfSeenEntriesFactory(tracker, absent()))

    def "should add all untracked entries"()
    {
        given:
        feedEndpoint.get() >> reader('/feedWithEntries_2_1.json')
        givenTracking(["2": UNTRACKED, "1" : UNTRACKED])

        when:
        availableEntriesTracker.trackNewEntries()

        then:
        1 * tracker.track(seenEntry('2', new DateTime(2013, 5, 24, 0, 0, 0, 0)))
        1 * tracker.track(seenEntry('1', new DateTime(2013, 5, 23, 0, 0, 0, 0)))
    }

    def "should add all untracked entries provided entries occurred after given earliest date restriction"()
    {
        given:
        def storeWithRestrictionOnEarliestDate = new AvailableFeedEntriesTracker(feedEndpoint, feedEndpointFactory, tracker, halReader, nonContiguousEntryTracker, new PageOfSeenEntriesFactory(tracker, earliestEntryDate()))
        feedEndpoint.get() >> reader('/feedWithEntries_2_1.json')
        givenTracking(["2": UNTRACKED, "1" : UNTRACKED])

        when:
        storeWithRestrictionOnEarliestDate.trackNewEntries()

        then:
        1 * tracker.track(seenEntry('2', new DateTime(2013, 5, 24, 0, 0, 0, 0)))
    }

    def "should return only untracked entries provided feed contains both tracked and untracked"()
    {
        given:
        feedEndpoint.get() >> reader('/feedWithEntries_4_3_2_1.json')
        givenTracking(["4": UNTRACKED, "3" : UNTRACKED, "2" : UNTRACKED, "1" : TRACKED])

        when:
        availableEntriesTracker.trackNewEntries()

        then:
        3 * tracker.track(_ as SeenEntry)
    }

    def "should walk to next feed page if all entries in the current page are untracked"()
    {
        given:
        feedEndpoint.get() >>> [reader('/feedWithNextLinkAndEntries_8_7.json'), reader('/feedWithNextLinkAndEntries_8_7.json')]
        secondPageEndpoint.get() >>> [reader('/feedWithPrevNextLinkAndEntries_6_5.json'), reader('/feedWithPrevNextLinkAndEntries_6_5.json')]
        thirdPageEndpoint.get() >> reader('/feedWithPrevLinkAndEntries_4_3_2_1.json')
        feedEndpointFactory.create(_ as String) >>> [secondPageEndpoint, thirdPageEndpoint, secondPageEndpoint, feedEndpoint]
        givenTracking(["8": UNTRACKED, "7": UNTRACKED, "6": UNTRACKED, "5": UNTRACKED, "4": UNTRACKED, "3" : UNTRACKED, "2" : TRACKED, "1" : TRACKED])

        when:
        availableEntriesTracker.trackNewEntries()

        then:
        1 * tracker.track(seenEntry('8', new DateTime(2013, 5, 26, 13, 0, 0)))
        1 * tracker.track(seenEntry('7', new DateTime(2013, 5, 26, 0, 0, 0)))
        1 * tracker.track(seenEntry('6', new DateTime(2013, 5, 25, 13, 0, 0)))
        1 * tracker.track(seenEntry('5', new DateTime(2013, 5, 25, 0, 0, 0)))
        1 * tracker.track(seenEntry('4', new DateTime(2013, 5, 24, 0, 0, 0)))
        1 * tracker.track(seenEntry('3', new DateTime(2013, 5, 23, 0, 0, 0)))
        0 * tracker.track(_ as SeenEntry)
    }

    def "should return none when tracker already contains all entries"()
    {
        given:
        feedEndpoint.get() >> reader('/feedWithEntries_2_1.json')
        givenTracking(["2" : TRACKED, "1" : TRACKED])

        when:
        availableEntriesTracker.trackNewEntries()

        then:
        0 * tracker.track(_ as SeenEntry)
    }

    private static seenEntry(final String id, final DateTime dateTime)
    {
        return new SeenEntry(EntryId.of(id), dateTime)
    }

    private static reader(String resourcePath)
    {
        return Optional.of(new InputStreamReader(Resources.getResourceAsStream(resourcePath)))
    }

    private static Optional<EarliestEntryLimit> earliestEntryDate()
    {
        return Optional.of(new EarliestEntryLimit(new DateTime(2013, 5, 23, 0, 0, 1)))
    }

    private givenTracking(final Map<String, Boolean> trackingResults)
    {
        trackingResults.entrySet().each {
            tracker.isTracked(EntryId.of(it.key)) >> it.value
        }
    }
}
