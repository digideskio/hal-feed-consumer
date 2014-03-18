package com.qmetric.feed.consumer
import com.google.common.base.Optional
import com.qmetric.feed.consumer.store.AlreadyConsumingException
import com.qmetric.feed.consumer.store.FeedTracker
import com.qmetric.hal.reader.HalResource
import spock.lang.Specification
import spock.lang.Unroll

import static net.java.quickcheck.generator.PrimitiveGeneratorSamples.anyString

class EntryConsumerImplTest extends Specification {

    private static final int MAX_RETRIES = 10

    private static final int RETRIES = 1

    final consumeAction = Mock(ConsumeAction)

    final resourceResolver = Mock(ResourceResolver)

    final feedTracker = Mock(FeedTracker)

    final listener = Mock(EntryConsumerListener)

    final consumer = new EntryConsumerImpl(feedTracker, consumeAction, resourceResolver, [listener], Optional.of(MAX_RETRIES))

    def entry = new TrackedEntry(EntryId.of(anyString()), RETRIES)

    def resource = Mock(HalResource)

    def "should consume entry"()
    {
        when:
        consumer.consume(entry)

        then:
        1 * feedTracker.markAsConsuming(entry.id)
        1 * resourceResolver.resolve(entry.id) >> resource
        1 * consumeAction.consume(new FeedEntry(resource, RETRIES)) >> Result.successful()
        1 * feedTracker.markAsConsumed(entry.id)
    }

    def "should use failure result from consumption to determine whether to retry"()
    {
        given:
        consumeAction.consume(_) >> result

        when:
        consumer.consume(entry)

        then:
        1 * feedTracker.fail(_, shouldRetry)
        0 * feedTracker.markAsConsumed(entry.id)
        0 * listener.consumed(entry.id)

        where:
        result | shouldRetry
        Result.retryUnsuccessful() | true
        Result.abortUnsuccessful() | false
    }

    def "should not consume entry if already being consumed by another consumer"()
    {
        when:
        consumer.consume(entry)

        then:
        1 * feedTracker.markAsConsuming(entry.id) >> { throw new AlreadyConsumingException() }
        0 * consumeAction._
        0 * feedTracker._
        thrown(AlreadyConsumingException)
    }

    @Unroll def "should mark entry as failed if error occurs whilst consuming entry (mark fail should also revert consuming)"()
    {
        when:
        consumer.consume(new TrackedEntry(EntryId.of(anyString()), retries))

        then:
        1 * consumeAction.consume(_) >> { throw new Exception() }
        1 * feedTracker.fail(_, shouldRetry)
        0 * feedTracker.markAsConsumed(_)
        thrown(Exception)

        where:
        retries | shouldRetry
        MAX_RETRIES - 1 | true
        MAX_RETRIES     | false
        MAX_RETRIES + 1 | false
    }

    def "should retry to set consumed state on error"()
    {
        given:
        consumeAction.consume(_) >> Result.successful()

        when:
        consumer.consume(entry)

        then:
        1 * feedTracker.markAsConsumed(_) >> { throw new Exception() }

        then:
        1 * feedTracker.markAsConsumed(_)
    }

    def "should notify listeners on consuming entry"()
    {
        given:
        consumeAction.consume(_) >> Result.successful()

        when:
        consumer.consume(entry)

        then:
        1 * listener.consumed(entry.id)
    }
}