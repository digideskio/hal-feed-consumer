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
        EntryId entryId1 = anyEntryId()
        EntryId entryId2 = anyEntryId()
        EntryId entryId3 = anyEntryId()
        List entryIds = [entryId1, entryId2, entryId3]

        when:
        consumer.consume()

        then:
        1 * feedTracker.getItemsToBeConsumed() >> entryIds
        entryIds.each { EntryId l -> 1 * entryConsumer.consume(l) }
        1 * listener.consumed(entryIds)
    }

    def "should notify listeners even if we have an empty list"()
    {
        when:
        consumer.consume()

        then:
        1 * feedTracker.getItemsToBeConsumed() >> []
        0 * entryConsumer.consume(_)
        1 * listener.consumed([])
    }

    def "should skip entries already consumed by other consumers"()
    {
        when:
        consumer.consume()

        then:
        1 * feedTracker.getItemsToBeConsumed() >> [anyEntryId(), anyEntryId()]
        1 * entryConsumer.consume(_) >> { throw new AlreadyConsumingException() }
        1 * entryConsumer.consume(_)
    }

    def "should skip entries that can't be consumed"()
    {
        when:
        consumer.consume()

        then:
        1 * feedTracker.getItemsToBeConsumed() >> [anyEntryId(), anyEntryId()]
        1 * entryConsumer.consume(_) >> { throw new RuntimeException() }
        1 * entryConsumer.consume(_)
    }

    private static EntryId anyEntryId()
    {
        EntryId.of(anyString())
    }
}
