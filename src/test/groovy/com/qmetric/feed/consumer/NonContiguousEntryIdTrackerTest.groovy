package com.qmetric.feed.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.io.Resources
import com.qmetric.feed.consumer.store.FeedTracker
import com.qmetric.hal.reader.HalReader
import org.joda.time.DateTime
import spock.lang.Specification
import com.google.common.base.Optional

import static com.google.common.base.Optional.absent

class NonContiguousEntryIdTrackerTest extends Specification {

    static final TRACKED = true

    static final UNTRACKED = false

    final feedEndpoint = Mock(FeedEndpoint)

    final secondPageEndpoint = Mock(FeedEndpoint)

    final tracker = Mock(FeedTracker)

    final feedEndpointFactory = Mock(FeedEndpointFactory)

    final halReader = new HalReader(new ObjectMapper())

    final availableEntriesTracker = new AvailableFeedEntriesTracker(feedEndpoint, feedEndpointFactory, tracker, halReader, new NonContiguousEntryIdTracker(tracker), new PageOfSeenEntriesFactory(tracker, absent()))

    def "should track missing entries where entry ids not contiguous (unlikely, but maybe the case under heavy load)"()
    {
        given:
        feedEndpoint.get() >> reader('/feedWithNonContiguousEntries_6_4_2_1.json')
        givenTracking(["6": UNTRACKED, "4": UNTRACKED, "2": UNTRACKED, "1": UNTRACKED])

        when:
        availableEntriesTracker.trackNewEntries()

        then:
        1 * tracker.track(seenEntry('6', new DateTime(2013, 5, 25, 0, 0, 0)))
        1 * tracker.track(seenEntry('5', new DateTime(2013, 5, 25, 0, 0, 0)))
        1 * tracker.track(seenEntry('4', new DateTime(2013, 5, 24, 0, 0, 0)))
        1 * tracker.track(seenEntry('3', new DateTime(2013, 5, 24, 0, 0, 0)))
        1 * tracker.track(seenEntry('2', new DateTime(2013, 5, 22, 13, 0, 0)))
        1 * tracker.track(seenEntry('1', new DateTime(2013, 5, 22, 0, 0, 0)))
        0 * tracker.track(_ as SeenEntry)
    }

    def "should track missing entries where entry ids not contiguous across page boundaries (unlikely, but maybe the case under heavy load)"()
    {
        given:
        feedEndpoint.get() >>> [reader('/feedWithNextLinkAndEntries_8_7.json'), reader('/feedWithNextLinkAndEntries_8_7.json')]
        secondPageEndpoint.get() >> reader('/feedWithPrevLinkAndEntries_4_3_2_1.json')
        feedEndpointFactory.create(_ as String) >>> [secondPageEndpoint, feedEndpoint]
        givenTracking(["8": UNTRACKED, "7": UNTRACKED, "4": UNTRACKED, "3": UNTRACKED, "2": TRACKED, "1": TRACKED])

        when:
        availableEntriesTracker.trackNewEntries()

        then:
        1 * tracker.track(seenEntry('8', new DateTime(2013, 5, 26, 13, 0, 0)))
        1 * tracker.track(seenEntry('7', new DateTime(2013, 5, 26, 0, 0, 0)))
        1 * tracker.track(seenEntry('6', new DateTime(2013, 5, 26, 0, 0, 0)))
        1 * tracker.track(seenEntry('5', new DateTime(2013, 5, 26, 0, 0, 0)))
        1 * tracker.track(seenEntry('4', new DateTime(2013, 5, 24, 0, 0, 0)))
        1 * tracker.track(seenEntry('3', new DateTime(2013, 5, 23, 0, 0, 0)))
        0 * tracker.track(_ as SeenEntry)
    }

    def "should track missing entries where entry ids not contiguous where all already tracked on last page (unlikely, but maybe the case under heavy load)"()
    {
        given:
        feedEndpoint.get() >>> [reader('/feedWithNextLinkAndEntries_8_7.json'), reader('/feedWithNextLinkAndEntries_8_7.json')]
        secondPageEndpoint.get() >> reader('/feedWithPrevLinkAndEntries_4_3_2_1.json')
        feedEndpointFactory.create(_ as String) >>> [secondPageEndpoint, feedEndpoint]
        givenTracking(["8": UNTRACKED, "7": UNTRACKED, "4": TRACKED, "3": TRACKED, "2": TRACKED, "1": TRACKED])

        when:
        availableEntriesTracker.trackNewEntries()

        then:
        1 * tracker.track(seenEntry('8', new DateTime(2013, 5, 26, 13, 0, 0)))
        1 * tracker.track(seenEntry('7', new DateTime(2013, 5, 26, 0, 0, 0)))
        1 * tracker.track(seenEntry('6', new DateTime(2013, 5, 26, 0, 0, 0)))
        1 * tracker.track(seenEntry('5', new DateTime(2013, 5, 26, 0, 0, 0)))
        0 * tracker.track(_ as SeenEntry)
    }

    def "should track missing entries where entry ids not contiguous intertwined with already tracked entries (unlikely, but maybe the case under heavy load)"()
    {
        given:
        feedEndpoint.get() >> reader('/feedWithNonContiguousEntries_6_4_2_1.json')
        givenTracking(["6": TRACKED, "4": UNTRACKED, "2": TRACKED, "1": UNTRACKED])

        when:
        availableEntriesTracker.trackNewEntries()

        then:
        1 * tracker.track(seenEntry('5', new DateTime(2013, 5, 25, 0, 0, 0, 0)))
        1 * tracker.track(seenEntry('4', new DateTime(2013, 5, 24, 0, 0, 0, 0)))
        1 * tracker.track(seenEntry('3', new DateTime(2013, 5, 24, 0, 0, 0, 0)))
        1 * tracker.track(seenEntry('1', new DateTime(2013, 5, 22, 0, 0, 0, 0)))
        0 * tracker.track(_ as SeenEntry)
    }

    def "should not track any missing entries when ids are contiguous"()
    {
        given:
        feedEndpoint.get() >> reader('/feedWithEntries_2_1.json')
        givenTracking(["2": UNTRACKED, "1": UNTRACKED])

        when:
        availableEntriesTracker.trackNewEntries()

        then:
        1 * tracker.track(seenEntry('2', new DateTime(2013, 5, 24, 0, 0, 0, 0)))
        1 * tracker.track(seenEntry('1', new DateTime(2013, 5, 23, 0, 0, 0, 0)))
        0 * tracker.track(_ as SeenEntry)
    }

    private static seenEntry(final String id, final DateTime dateTime)
    {
        return new SeenEntry(EntryId.of(id), dateTime)
    }

    private static reader(final String resourcePath)
    {
        return Optional.of(new InputStreamReader(Resources.getResourceAsStream(resourcePath)))
    }

    private givenTracking(final Map<String, Boolean> trackingResults)
    {
        trackingResults.entrySet().each {
            tracker.isTracked(EntryId.of(it.key)) >> it.value
        }
    }
}
