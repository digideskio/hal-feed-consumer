package com.qmetric.feed.consumer

import com.google.common.base.Optional
import com.google.common.io.Resources
import com.qmetric.feed.consumer.store.AlreadyConsumingException
import com.qmetric.feed.consumer.store.FeedTracker
import spock.lang.Specification

class FeedConsumerImplTest extends Specification
{

    private static final url = "http://host/feed"

    final endpoint = Mock(FeedEndpoint)

    final entryConsumer = Mock(EntryConsumer)

    final consumedStore = Mock(FeedTracker)

    final listener = Mock(FeedPollingListener)

    final feedEndpointFactory = Mock(FeedEndpointFactory)

    FeedConsumerImpl consumer

    def setup()
    {
        feedEndpointFactory.create(url) >> endpoint
        endpoint.get() >> reader('/feedWithThreeEntries.json')
        consumer = new FeedConsumerImpl(url, feedEndpointFactory, entryConsumer, consumedStore, Optional.absent(), [listener])
    }

    def "should consume all unconsumed entries"()
    {
        given:
        consumedStore.notAlreadyConsumed(_) >> true

        when:
        consumer.consume()

        then:
        3 * entryConsumer.consume(_)
    }

    def "should ignore already consumed entries"()
    {
        given:
        consumedStore.notAlreadyConsumed(_) >> false

        when:
        consumer.consume()

        then:
        0 * entryConsumer.consume(_)
    }

    def "should notify listeners after polling feed for new entries"()
    {
        given:
        consumedStore.notAlreadyConsumed(_) >> true

        when:
        consumer.consume()

        then:
        3 * entryConsumer.consume(_)
        1 * listener.consumed({ List entries -> entries.size() == 3 })
    }

    def "should notify listeners after polling feed for new entries even if we have an empty list"()
    {
        given:
        consumedStore.notAlreadyConsumed(_) >> false

        when:
        consumer.consume()

        then:
        1 * listener.consumed([])
    }

    def "should skip entries already consumed by other consumers"()
    {
        given:
        consumedStore.notAlreadyConsumed(_) >> true

        when:
        consumer.consume()

        then:
        1 * entryConsumer.consume(_) >> { throw new AlreadyConsumingException() }
        2 * entryConsumer.consume(_)
    }

    def "should skip entries that can't be consumed"()
    {
        given:
        consumedStore.notAlreadyConsumed(_) >> true

        when:
        consumer.consume()

        then:
        1 * entryConsumer.consume(_) >> { throw new RuntimeException() }
        2 * entryConsumer.consume(_)
    }

    private static InputStreamReader reader(String resource)
    {
        new InputStreamReader(Resources.getResourceAsStream(resource), 'UTF-8')
    }
}
