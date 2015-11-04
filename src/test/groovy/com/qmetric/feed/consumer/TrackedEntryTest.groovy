package com.qmetric.feed.consumer

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


    @Shared private DateTime someDate = new DateTime(2014, 2, 14, 12, 0, 0, 0)
}
