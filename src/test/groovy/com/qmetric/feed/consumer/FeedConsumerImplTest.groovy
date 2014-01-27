package com.qmetric.feed.consumer
import com.qmetric.feed.consumer.store.AlreadyConsumingException
import com.qmetric.feed.consumer.store.FeedTracker
import spock.lang.Specification

import static net.java.quickcheck.generator.PrimitiveGeneratorSamples.anyString

class FeedConsumerImplTest extends Specification {

    final entryConsumer = Mock(EntryConsumer)

    final feedTracker = Mock(FeedTracker)

    final listener = Mock(FeedPollingListener)

    FeedConsumerImpl consumer

    def setup()
    {
        consumer = new FeedConsumerImpl(entryConsumer, feedTracker, [listener])
    }

    def "should consume provided unconsumed entries in sequence and notify the listeners"()
    {
        given:
        TrackedEntry entry1 = anyEntry()
        TrackedEntry entry2 = anyEntry()
        TrackedEntry entry3 = anyEntry()
        List entries = [entry1, entry2, entry3]

        when:
        consumer.consume()

        then:
        1 * feedTracker.getEntriesToBeConsumed() >> entries
        entries.each { TrackedEntry l -> 1 * entryConsumer.consume(l) }
        1 * listener.consumed(entries)
    }

    def "should notify listeners even if we have an empty list"()
    {
        when:
        consumer.consume()

        then:
        1 * feedTracker.getEntriesToBeConsumed() >> []
        0 * entryConsumer.consume(_)
        1 * listener.consumed([])
    }

    def "should skip entries already consumed by other consumers"()
    {
        when:
        consumer.consume()

        then:
        1 * feedTracker.getEntriesToBeConsumed() >> [anyEntry(), anyEntry()]
        1 * entryConsumer.consume(_) >> { throw new AlreadyConsumingException() }
        1 * entryConsumer.consume(_)
    }

    def "should skip entries that can't be consumed"()
    {
        when:
        consumer.consume()

        then:
        1 * feedTracker.getEntriesToBeConsumed() >> [anyEntry(), anyEntry()]
        1 * entryConsumer.consume(_) >> { throw new RuntimeException() }
        1 * entryConsumer.consume(_)
    }

    private static TrackedEntry anyEntry()
    {
        new TrackedEntry(EntryId.of(anyString()), 1)
    }
}
