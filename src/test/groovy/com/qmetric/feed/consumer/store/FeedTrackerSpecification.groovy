package com.qmetric.feed.consumer.store

import com.qmetric.feed.consumer.DateTimeSource
import com.qmetric.feed.consumer.EntryId
import com.qmetric.feed.consumer.SeenEntry
import com.qmetric.feed.consumer.TrackedEntry
import org.joda.time.DateTime
import spock.lang.Specification

import static com.qmetric.feed.consumer.store.TrackedEntryBuilder.trackedEntryBuilder

abstract class FeedTrackerSpecification extends Specification {

    // TODO: seenAt - checked using retry strategy

    def 'track entries'() {
        given:
        assert !feedTracker.isTracked(seenEntry1.id)
        assert !feedTracker.isTracked(seenEntry2.id)

        when:
        feedTracker.track(seenEntry1)

        then:
        feedTracker.isTracked(seenEntry1.id)
        !feedTracker.isTracked(seenEntry2.id)
    }

    def 'consider tracked entries as ready to be consumed'() {
        given:
        assert count(feedTracker.getEntriesToBeConsumed()) == 0

        when:
        feedTracker.track(seenEntry1)
        feedTracker.track(seenEntry2)

        then:
        count(feedTracker.getEntriesToBeConsumed()) == 2
    }

    def 'use information from seen entries to create tracked entries'() {
        when:
        feedTracker.track(seenEntry2)
        feedTracker.track(seenEntry1)

        then:
        Collection<EntryId> storedEntryIds = feedTracker.getEntriesToBeConsumed().id
        storedEntryIds.size() == 2
        storedEntryIds.containsAll([seenEntry1.id, seenEntry2.id])
    }

    def 'be idempotent for initial tracking - keep first tracked entry if the same tracked many times'() {
        given:
        DateTime timeOfFirstTracking = someTime.plusDays(1)
        DateTime timeOfSecondTracking = someTime.plusDays(2)
        dateTimeSource.now() >>> [timeOfFirstTracking, timeOfSecondTracking]

        when:
        feedTracker.track(seenEntry1)
        feedTracker.track(seenEntry1)

        then:
        takeOne(feedTracker.getEntriesToBeConsumed()).created == timeOfFirstTracking
    }

    def 'not consider entries already being consumed as available to be consumed again'() {
        given:
        feedTracker.track(seenEntry1)
        feedTracker.track(seenEntry2)

        when:
        feedTracker.markAsConsuming(seenEntry1.id)

        then:
        feedTracker.getEntriesToBeConsumed().id == [seenEntry2.id]
    }


    def 'prevent multiple consumers from consuming an entry already being consumed'() {
        given:
        feedTracker.track(seenEntry1)
        feedTracker.markAsConsuming(seenEntry1.id)

        when:
        feedTracker.markAsConsuming(seenEntry1.id)

        then:
        thrown(AlreadyConsumingException)
    }

    def 'still consider a being consumed entry as tracked'() {
        given:
        feedTracker.track(seenEntry1)

        when:
        feedTracker.markAsConsuming(seenEntry1.id)

        then:
        feedTracker.isTracked(seenEntry1.id)
    }

    def 'throw exception when tries to track not existing entry'() {
        given:
        !feedTracker.isTracked(seenEntry1.id)

        when:
        feedTracker.markAsConsuming(seenEntry1.id)

        then:
        thrown(EntryNotTrackedException)
    }

    def 'NOT consider permanently failed entries as available to be consumed'()
    {
        given:
        feedTracker.track(seenEntry1)
        assert count(feedTracker.getEntriesToBeConsumed()) == 1
        TrackedEntry trackedEntry = takeOne(feedTracker.getEntriesToBeConsumed())
        boolean scheduleRetry = false

        when:
        feedTracker.fail(trackedEntry, scheduleRetry)

        then:
        count(feedTracker.getEntriesToBeConsumed()) == 0
    }


    def 'still consider entries failed with potential retry as available to be consumed'()
    {
        given:
        feedTracker.track(seenEntry1)
        assert count(feedTracker.getEntriesToBeConsumed()) == 1
        TrackedEntry trackedEntry = takeOne(feedTracker.getEntriesToBeConsumed())
        boolean scheduleRetry = true

        when:
        feedTracker.fail(trackedEntry, scheduleRetry)

        then:
        count(feedTracker.getEntriesToBeConsumed()) == 1
        takeOne(feedTracker.getEntriesToBeConsumed()).id == trackedEntry.id
    }

    def 'NOT consider consumed entries as available to be consumed again'() {
        given:
        feedTracker.track(seenEntry1)
        assert count(feedTracker.getEntriesToBeConsumed()) == 1

        when:
        feedTracker.markAsConsumed(seenEntry1.id)

        then:
        count(feedTracker.getEntriesToBeConsumed()) == 0
    }

    def 'increase retries count after each failure'() {
        given:
        feedTracker.track(seenEntry1)
        TrackedEntry trackedEntry = takeOne(feedTracker.getEntriesToBeConsumed())
        assert trackedEntry.retries == 0
        boolean scheduleRetry = true

        when:
        feedTracker.fail(trackedEntry, scheduleRetry)
        trackedEntry = takeOne(feedTracker.getEntriesToBeConsumed())
        feedTracker.fail(trackedEntry, scheduleRetry)

        then:
        takeOne(feedTracker.getEntriesToBeConsumed()).retries == 2
    }

    def "prefer older entries over recently tracked or failed ones so that each one has a chance to be process if a limit imposed"()
    {
        given:
        DateTime currentTime = someTime
        dateTimeSource.now() >>> tenMillisecondsIntervals(currentTime)
//        println dateTimeSource.now()
//        println dateTimeSource.now()
//        println dateTimeSource.now()
//        println dateTimeSource.now()
//        println dateTimeSource.now()
        final seenEntryId = trackNewEntry('seenEntry', currentTime.minusSeconds(15))
        final failedEntryId = trackNewEntry('failedEntry', currentTime.minusSeconds(14))
        final entryBeingConsumedId = trackNewEntry('entryBeingConsumed', currentTime.minusSeconds(12))
        final consumedEntryId = trackNewEntry('consumedEntry', currentTime.minusSeconds(11))
        final abortedEntryId = trackNewEntry('abortedEntry', currentTime.minusSeconds(10))

        feedTracker.markAsConsuming(failedEntryId)
        TrackedEntry failedEntry = trackedEntryBuilder().withEntryId(failedEntryId).build()
        feedTracker.fail(failedEntry, true)

        feedTracker.markAsConsuming(abortedEntryId)
        TrackedEntry abortedEntry = trackedEntryBuilder().withEntryId(abortedEntryId).build()
        feedTracker.fail(abortedEntry, false)

        feedTracker.markAsConsuming(consumedEntryId)
        feedTracker.markAsConsumed(consumedEntryId)

        feedTracker.markAsConsuming(entryBeingConsumedId)

        final anotherSeenEntryId = trackNewEntry('anotherSeenEntry', currentTime.plusSeconds(10))

        when:
        final entries = feedTracker.getEntriesToBeConsumed()

        then:
        entries.id == [seenEntryId, failedEntryId, anotherSeenEntryId]
    }

    private static List<DateTime> tenMillisecondsIntervals(DateTime currentTime) {
        (1..100).collect { currentTime.plusMillis( 10 * it) }
    }

    private EntryId trackNewEntry(String id, DateTime entryDateTime)
    {
        EntryId entryId = EntryId.of("${id}-${uniqueString()}")
        feedTracker.track(new SeenEntry(entryId, entryDateTime))

        entryId
    }

    private static String uniqueString() {
        UUID.randomUUID().toString()
    }

    private static int count(Iterable<Object> items) {
        items.iterator().hasNext() ? (int) items.collect{1}.sum() : 0;
    }

    private static TrackedEntry takeOne(Iterable<TrackedEntry> entries) {
        return entries.iterator().next()
    }

    def setup() {
        feedTracker = feedTrackedImplementation()
    }

    protected abstract FeedTracker feedTrackedImplementation()

    protected SeenEntry seenEntry1 = new SeenEntry(EntryId.of("1"), new DateTime())
    protected SeenEntry seenEntry2 = new SeenEntry(EntryId.of("2"), new DateTime())
    protected DateTimeSource dateTimeSource = Stub(DateTimeSource)
    protected DateTime someTime = DateTime.now()
    protected FeedTracker feedTracker
}
