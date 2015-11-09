package com.qmetric.feed.consumer.store.referenceimplementation

import com.qmetric.feed.consumer.EntryId
import com.qmetric.feed.consumer.SeenEntry
import com.qmetric.feed.consumer.store.EntryNotTrackedException
import com.qmetric.feed.consumer.store.FeedTracker
import com.qmetric.feed.consumer.store.FeedTrackerSpecification
import org.joda.time.DateTime

class InMemoryFeedTrackerAcceptanceTest extends FeedTrackerSpecification {

    def 'be idempotent for initial tracking - keep first tracked entry if the same tracked many times'() {
        given:
        DateTime timeOfSeenEntry = someTime.minusMinutes(10)
        SeenEntry entryToBeTracked = new SeenEntry(EntryId.of("1"), timeOfSeenEntry)

        when:
        feedTracker.track(entryToBeTracked)
        feedTracker.track(entryToBeTracked)

        then:
        takeOne(feedTracker.getEntriesToBeConsumed()).created == timeOfSeenEntry
    }

    def 'throw exception when tries to track not existing entry'() {
        given:
        !feedTracker.isTracked(seenEntry1.id)

        when:
        feedTracker.markAsConsuming(seenEntry1.id)

        then:
        thrown(EntryNotTrackedException)
    }

    @Override
    protected FeedTracker feedTrackedImplementation() {
        new InMemoryFeedTracker(dateTimeSource)
    }
}
