package com.qmetric.feed.consumer.retry
import com.qmetric.feed.consumer.DateTimeSource
import com.qmetric.feed.consumer.EntryConsumer
import com.qmetric.feed.consumer.TrackedEntry
import org.joda.time.DateTime
import spock.lang.Specification

import static com.qmetric.feed.consumer.store.TrackedEntryBuilder.buildEntryThatNeverFailedYet
import static com.qmetric.feed.consumer.store.TrackedEntryBuilder.trackedEntryBuilder

class RetryStrategyAwareEntryConsumerTest extends Specification {

    def "should use provided consumer to consume an entry"() {
        given:
        TrackedEntry trackedEntry = trackedEntryBuilder().build()
        EntryConsumer retryStrategyAwareEntryConsumer = new RetryStrategyAwareEntryConsumer(wrappedEntryConsumer, alwaysRetryingRetryStrategy, dateTimeSource)

        when:
        boolean returnedValue = retryStrategyAwareEntryConsumer.consume(trackedEntry)

        then:
        1 * wrappedEntryConsumer.consume(trackedEntry) >> valueReturnedByWrappedEntryConsumer
        returnedValue == valueReturnedByWrappedEntryConsumer

        where:
        valueReturnedByWrappedEntryConsumer << [true, false]
    }

    def "should return false and not consume trackedEntry if strategy disallows it"() {
        given:
        TrackedEntry trackedEntryThatFailedOnce = trackedEntryBuilder().withRetries(1).build()
        EntryConsumer retryStrategyAwareEntryConsumer = new RetryStrategyAwareEntryConsumer(wrappedEntryConsumer, alwaysRejectingRetryStrategy, dateTimeSource)

        when:
        boolean returnedValue = retryStrategyAwareEntryConsumer.consume(trackedEntryThatFailedOnce)

        then:
        0 * wrappedEntryConsumer._
        !returnedValue
    }

    def "should consume trackedEntry if it is not a retry entry, even if the strategy would reject it"() {
        given:
        TrackedEntry trackedEntryThatNeverFailedYet = buildEntryThatNeverFailedYet()
        EntryConsumer retryStrategyAwareEntryConsumer = new RetryStrategyAwareEntryConsumer(wrappedEntryConsumer, alwaysRejectingRetryStrategy, dateTimeSource)

        when:
        retryStrategyAwareEntryConsumer.consume(trackedEntryThatNeverFailedYet)

        then:
        1 * wrappedEntryConsumer.consume(trackedEntryThatNeverFailedYet)
    }

    def "should apply retry strategy and FILTER OUT entries that should NOT be processed"() {
        given:
        RetryStrategy retryStrategy = Mock(RetryStrategy)
        EntryConsumer retryStrategyAwareEntryConsumer = new RetryStrategyAwareEntryConsumer(wrappedEntryConsumer, retryStrategy, dateTimeSource)
        int retriesA = 1
        int retriesB = 5
        int retriesC = 7
        DateTime timeA = currentTime.minusHours(1)
        DateTime timeB = currentTime.minusHours(5)
        DateTime timeC = currentTime.minusHours(7)
        TrackedEntry trackedEntryA = trackedEntryBuilder().withRetries(retriesA).withSeenAt(timeA).build()
        TrackedEntry trackedEntryB = trackedEntryBuilder().withRetries(retriesB).withSeenAt(timeB).build()
        TrackedEntry trackedEntryC = trackedEntryBuilder().withRetries(retriesC).withSeenAt(timeC).build()
        retryStrategy.canRetry(retriesA, timeA, currentTime) >> true
        retryStrategy.canRetry(retriesB, timeB, currentTime) >> false
        retryStrategy.canRetry(retriesC, timeC, currentTime) >> true

        when:
        retryStrategyAwareEntryConsumer.consume(trackedEntryA)
        retryStrategyAwareEntryConsumer.consume(trackedEntryB)
        retryStrategyAwareEntryConsumer.consume(trackedEntryC)

        then:
        1 * wrappedEntryConsumer.consume(trackedEntryA)
        0 * wrappedEntryConsumer.consume(trackedEntryB)
        1 * wrappedEntryConsumer.consume(trackedEntryC)
        0 * wrappedEntryConsumer._
    }

    private EntryConsumer wrappedEntryConsumer = Mock(EntryConsumer)
    private DateTimeSource dateTimeSource = Stub(DateTimeSource)
    private DateTime currentTime = DateTime.now()
    private AlwaysRetryingRetryStrategy alwaysRetryingRetryStrategy = new AlwaysRetryingRetryStrategy()
    private RetryStrategy alwaysRejectingRetryStrategy = Mock(RetryStrategy)

    def setup() {
        dateTimeSource.now() >> currentTime
        alwaysRejectingRetryStrategy.canRetry(*_) >> false
    }
}
