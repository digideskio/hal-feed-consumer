package com.qmetric.feed.consumer

import com.qmetric.feed.consumer.retry.RetryStrategy
import org.joda.time.DateTime
import spock.lang.Shared
import spock.lang.Specification

import static com.qmetric.feed.consumer.store.TrackedEntryBuilder.trackedEntryBuilder

class TrackedEntryTest extends Specification {

    def "should be compared using id only"() {
        given:
        TrackedEntry trackedEntry1 = trackedEntryBuilder().withEntryId(EntryId.of("1")).withCreated(someDate).withRetries(0).build()
        TrackedEntry trackedEntry2 = trackedEntryBuilder().withEntryId(EntryId.of("1")).withCreated(someDate.plusDays(1)).withRetries(1).build()
        TrackedEntry trackedEntry3 = trackedEntryBuilder().withEntryId(EntryId.of("2")).withCreated(someDate).withRetries(0).build()

        expect:
        trackedEntry1 == trackedEntry1
        trackedEntry1.hashCode() == trackedEntry1.hashCode()

        trackedEntry1 == trackedEntry2
        trackedEntry1.hashCode() == trackedEntry2.hashCode()

        trackedEntry2 == trackedEntry1
        trackedEntry2.hashCode() == trackedEntry1.hashCode()

        trackedEntry1 != trackedEntry3
        trackedEntry3 != trackedEntry1
        trackedEntry2 != trackedEntry3
        trackedEntry3 != trackedEntry2
    }

    def "should be always allowed to process not yet processed entry without even asking retry strategy"() {
        given:
        TrackedEntry trackedEntry = trackedEntryBuilder().withRetries(0).build()

        when:
        boolean canBeProcessed = trackedEntry.canBeProcessed(retryStrategy, currentTime)

        then:
        canBeProcessed
        0 * retryStrategy._
    }

    def "should be always allowed to process when UNKNOWN seenAt value"() {
        given:
        TrackedEntry trackedEntry = trackedEntryBuilder().withSeenAt(null).withRetries(5).build()

        when:
        boolean canBeProcessed = trackedEntry.canBeProcessed(retryStrategy, currentTime)

        then:
        canBeProcessed
        0 * retryStrategy._
    }


    def "should decide based on provided strategy if can be processed"() {
        given:
        DateTime currentTime = DateTime.now()
        DateTime seenAt = currentTime.minusMinutes(10)
        int retries = 2

        TrackedEntry trackedEntry = trackedEntryBuilder().withSeenAt(seenAt).withRetries(retries).build()
        retryStrategy.canRetry(retries, seenAt, currentTime) >> canRetryAccordingToTheStrategy

        expect:
        trackedEntry.canBeProcessed(retryStrategy, currentTime) == shouldBeProcessed

        where:
        canRetryAccordingToTheStrategy || shouldBeProcessed
        true                           || true
        false                          || false
    }




    @Shared private DateTime someDate = new DateTime(2014, 2, 14, 12, 0, 0, 0)
    private RetryStrategy retryStrategy = Stub(RetryStrategy)
    private DateTime currentTime = DateTime.now()

    private static int retries() { 0 }
    private static EntryId entryId() { EntryId.of("1") }
}
