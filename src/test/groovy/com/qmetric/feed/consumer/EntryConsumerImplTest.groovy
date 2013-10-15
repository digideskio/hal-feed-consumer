package com.qmetric.feed.consumer

import com.google.common.io.Resources
import com.qmetric.feed.consumer.store.AlreadyConsumingException
import com.qmetric.feed.consumer.store.ConsumedStore
import com.theoryinpractise.halbuilder.DefaultRepresentationFactory
import spock.lang.Specification

class EntryConsumerImplTest extends Specification
{

    final consumeAction = Mock(ConsumeAction)

    final consumedStore = Mock(ConsumedStore)

    final listener = Mock(EntryConsumerListener)

    final feedEntry = new DefaultRepresentationFactory().readRepresentation(reader('/feedWithOneEntry.json'))

    final consumer = new EntryConsumerImpl(consumedStore, consumeAction, [listener])

    def "should consume entry"()
    {
        when:
        consumer.consume(feedEntry)

        then:
        1 * consumedStore.markAsConsuming(_)
        1 * consumeAction.consume(_)
        1 * consumedStore.markAsConsumed(_)
    }

    def "should not consume entry if already being consumed by another consumer"()
    {
        when:
        consumer.consume(feedEntry)

        then:
        1 * consumedStore.markAsConsuming(_) >> { throw new AlreadyConsumingException() }
        0 * consumeAction.consume(_)
        0 * consumedStore.markAsConsumed(_)
        0 * consumedStore.revertConsuming(_)
        thrown(AlreadyConsumingException)
    }

    def "should revert consuming state if error occurs whilst consuming entry"()
    {
        when:
        consumer.consume(feedEntry)

        then:
        1 * consumeAction.consume(_) >> { throw new Exception() }
        0 * consumedStore.markAsConsumed(_)
        1 * consumedStore.revertConsuming(_)
        thrown(Exception)
    }

    def "should retry to set consumed state on error"()
    {
        when:
        consumer.consume(feedEntry)

        then:
        1 * consumedStore.markAsConsumed(_) >> { throw new Exception() }

        then:
        1 * consumedStore.markAsConsumed(_)
    }

    def "should notify listeners on consuming entry"()
    {
        when:
        consumer.consume(feedEntry)

        then:
        1 * listener.consumed(feedEntry)
    }

    private static InputStreamReader reader(String s)
    {
        new InputStreamReader(Resources.getResourceAsStream(s))
    }
}
