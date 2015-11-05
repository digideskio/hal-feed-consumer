package com.qmetric.feed.consumer.retry

import org.joda.time.DateTime
import spock.lang.Shared
import spock.lang.Specification

class AlwaysRetryingRetryStrategyTest extends Specification {
    def "should always retry no matter what"() {
        expect:
        new AlwaysRetryingRetryStrategy().canRetry(retriesSoFar, seenAt, currentTime)

        where:
        retriesSoFar | seenAt | currentTime
        1            | time   | time.plusHours(1)
        1            | time   | time.minusHours(1)
        2            | time   | time.plusHours(1)
        2            | time   | time.minusHours(1)
    }

    @Shared private DateTime time = DateTime.now()
}
