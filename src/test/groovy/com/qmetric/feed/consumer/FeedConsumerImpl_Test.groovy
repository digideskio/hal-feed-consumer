package com.qmetric.feed.consumer

import com.qmetric.feed.consumer.store.AlreadyConsumingException
import com.qmetric.feed.consumer.store.FeedTracker
import com.theoryinpractise.halbuilder.api.Link
import com.theoryinpractise.halbuilder.api.RepresentationFactory
import spock.lang.Specification

import static net.java.quickcheck.generator.PrimitiveGeneratorSamples.anyInteger

class FeedConsumerImpl_Test extends Specification
{

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
        Link link1 = anyLink()
        Link link2 = anyLink()
        Link link3 = anyLink()
        List links = [link1, link2, link3]

        when:
        consumer.consume()

        then:
        1 * feedTracker.getItemsToBeConsumed() >> links
        links.each { Link l -> 1 * entryConsumer.consume(l) }
        1 * listener.consumed(links)
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
        1 * feedTracker.getItemsToBeConsumed() >> [anyLink(), anyLink()]
        1 * entryConsumer.consume(_) >> { throw new AlreadyConsumingException() }
        1 * entryConsumer.consume(_)
    }

    def "should skip entries that can't be consumed"()
    {
        when:
        consumer.consume()

        then:
        1 * feedTracker.getItemsToBeConsumed() >> [anyLink(), anyLink()]
        1 * entryConsumer.consume(_) >> { throw new RuntimeException() }
        1 * entryConsumer.consume(_)
    }

    private Link anyLink()
    {
        new Link(Mock(RepresentationFactory), 'self', "http://feed/${anyInteger()}")
    }
}
