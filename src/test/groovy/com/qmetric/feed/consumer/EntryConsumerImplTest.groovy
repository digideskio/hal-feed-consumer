package com.qmetric.feed.consumer

import com.qmetric.feed.consumer.store.AlreadyConsumingException
import com.qmetric.feed.consumer.store.FeedTracker
import com.theoryinpractise.halbuilder.api.Link
import com.theoryinpractise.halbuilder.api.ReadableRepresentation
import com.theoryinpractise.halbuilder.api.RepresentationFactory
import spock.lang.Specification

import static net.java.quickcheck.generator.PrimitiveGeneratorSamples.anyString

class EntryConsumerImplTest extends Specification
{

    final consumeAction = Mock(ConsumeAction)
    final resourceResolver = Mock(ResourceResolver)

    final feedTracker = Mock(FeedTracker)

    final listener = Mock(EntryConsumerListener)

    final consumer = new EntryConsumerImpl(feedTracker, consumeAction, resourceResolver, [listener])

    def link = new Link(Mock(RepresentationFactory), anyString(), anyString())
    def resource = Mock(ReadableRepresentation)

    def "should consume entry"()
    {
        when:
        consumer.consume(link)

        then:
        1 * feedTracker.markAsConsuming(link)
        1 * resourceResolver.resolve(link) >> resource
        1 * consumeAction.consume(resource)
        1 * feedTracker.markAsConsumed(link)
    }

    def "should not consume entry if already being consumed by another consumer"()
    {
        when:
        consumer.consume(link)

        then:
        1 * feedTracker.markAsConsuming(link) >> { throw new AlreadyConsumingException() }
        0 * consumeAction._
        0 * feedTracker._
        thrown(AlreadyConsumingException)
    }

    def "should mark entry as failed if error occurs whilst consuming entry (mark fail should also revert consuming)"()
    {
        when:
        consumer.consume(link)

        then:
        1 * consumeAction.consume(_) >> { throw new Exception() }
        1 * feedTracker.fail(_)
        0 * feedTracker.markAsConsumed(_)
        thrown(Exception)
    }

    def "should retry to set consumed state on error"()
    {
        when:
        consumer.consume(link)

        then:
        1 * feedTracker.markAsConsumed(_) >> { throw new Exception() }

        then:
        1 * feedTracker.markAsConsumed(_)
    }

    def "should notify listeners on consuming entry"()
    {
        when:
        consumer.consume(link)

        then:
        1 * listener.consumed(link)
    }
}