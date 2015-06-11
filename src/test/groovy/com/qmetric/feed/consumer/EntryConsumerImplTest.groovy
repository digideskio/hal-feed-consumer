package com.qmetric.feed.consumer

import com.google.common.base.Optional
import com.qmetric.feed.consumer.store.AlreadyConsumingException
import com.qmetric.feed.consumer.store.FeedTracker
import com.qmetric.hal.reader.HalResource
import org.joda.time.DateTime
import spock.lang.Specification
import spock.lang.Unroll

import static com.google.common.base.Optional.absent
import static java.util.concurrent.TimeUnit.MINUTES

class EntryConsumerImplTest extends Specification {

    private static final int MAX_RETRIES = 10

    private static final int RETRIES = 1

    private static final Interval MISSING_ENTRY_TIMEOUT = new Interval(15, MINUTES)

    final consumeAction = Mock(ConsumeAction)

    final resourceResolver = Mock(ResourceResolver)

    final feedTracker = Mock(FeedTracker)

    final listener = Mock(EntryConsumerListener)

    final dateTimeSource = Mock(DateTimeSource)

    final consumer = new EntryConsumerImpl(feedTracker, consumeAction, resourceResolver, [listener], Optional.of(MAX_RETRIES), MISSING_ENTRY_TIMEOUT, dateTimeSource)

    final dateTime = DateTime.now()

    def entry = new TrackedEntry(EntryId.of("1"), dateTime, RETRIES)

    def resource = Mock(HalResource)

    def "should consume entry"()
    {
        given:
        resourceResolver.resolve(_ as EntryId) >> Optional.of(resource)

        when:
        consumer.consume(entry)

        then:
        1 * feedTracker.markAsConsuming(entry.id)
        1 * consumeAction.consume(new FeedEntry(resource, RETRIES)) >> Result.successful()
        1 * feedTracker.markAsConsumed(entry.id)
    }

    def "should use failure result from consumption to determine whether to retry"()
    {
        given:
        resourceResolver.resolve(_ as EntryId) >> Optional.of(resource)
        consumeAction.consume(_) >> result

        when:
        consumer.consume(entry)

        then:
        1 * feedTracker.fail(_ as TrackedEntry, shouldRetry)
        0 * feedTracker.markAsConsumed(entry.id)
        0 * listener.consumed(entry.id)

        where:
        result | shouldRetry
        Result.retryUnsuccessful() | true
        Result.abortUnsuccessful() | false
    }

    def "should not consume entry if already being consumed by another consumer"()
    {
        given:
        resourceResolver.resolve(_ as EntryId) >> Optional.of(resource)

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
        given:
        resourceResolver.resolve(_ as EntryId) >> Optional.of(resource)

        when:
        consumer.consume(new TrackedEntry(EntryId.of("1"), dateTime, retries))

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
        resourceResolver.resolve(_ as EntryId) >> Optional.of(resource)
        consumeAction.consume(_) >> Result.successful()

        when:
        consumer.consume(entry)

        then:
        1 * feedTracker.markAsConsumed(_ as EntryId) >> { throw new Exception() }

        then:
        1 * feedTracker.markAsConsumed(_ as EntryId)
    }

    def "should notify listeners on consuming entry"()
    {
        given:
        resourceResolver.resolve(_ as EntryId) >> Optional.of(resource)
        consumeAction.consume(_) >> Result.successful()

        when:
        consumer.consume(entry)

        then:
        1 * listener.consumed(entry.id)
    }

    @Unroll def "should abort/retry when tracked entry does not exist in feed (entry might not exist temporarily during high load on feed-server)"()
    {
        given:
        dateTimeSource.now() >> currentDate
        final entry = new TrackedEntry(EntryId.of("1"), trackingDate, 0)
        resourceResolver.resolve(_ as EntryId) >> absent()

        when:
        consumer.consume(entry)

        then:
        1 * feedTracker.markAsConsuming(entry.id)
        0 * consumeAction.consume(new FeedEntry(resource, RETRIES)) >> Result.successful()
        0 * feedTracker.markAsConsumed(entry.id)
        1 * feedTracker.fail(entry, scheduleRetryForMissingEntry)

        where:
        currentDate                               | trackingDate                           | scheduleRetryForMissingEntry
        new DateTime(2015, 5, 1, 12, 15, 0, 0)    | new DateTime(2015, 5, 1, 12, 15, 0, 0) | true
        new DateTime(2015, 5, 1, 12, 29, 59, 999) | new DateTime(2015, 5, 1, 12, 15, 0, 0) | true
        new DateTime(2015, 5, 1, 12, 30, 0, 0)    | new DateTime(2015, 5, 1, 12, 15, 0, 0) | false
        new DateTime(2015, 5, 1, 12, 30, 0, 1)    | new DateTime(2015, 5, 1, 12, 15, 0, 0) | false
        new DateTime(2015, 5, 1, 12, 30, 0, 0)    | null                                   | false
    }
}