package com.qmetric.feed.consumer.retry

import com.qmetric.feed.consumer.Interval
import org.joda.time.DateTime
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class FibonacciDelayingRetryStrategyTest extends Specification {

    def "should always allow retry if no retries so far"() {
        given:
        int retriesSoFar = 0

        expect:
        retryStrategy.canRetry(retriesSoFar, someTime, someTime)
        retryStrategy.canRetry(retriesSoFar, someTime, someTime.plusMinutes(1))
        retryStrategy.canRetry(retriesSoFar, someTime, someTime.minusMinutes(1))
    }

    //@spock.lang.Unroll
    def "should increase interval based on Fibonacci sequence to a certain point, e.g. should wait at least #howManyMinutesShouldWait minutes after #retriesSoFar retries for base interval set to #intervalInMinutes minutes"() {
        given:
        DateTime timeOfLastOperation = someTime
        DateTime momentBeforeExpectedDelay = timeOfLastOperation.plusMinutes(howManyMinutesShouldWait).minusSeconds(1)
        DateTime momentAfterExpectedDelay = timeOfLastOperation.plusMinutes(howManyMinutesShouldWait).plusSeconds(1)

        expect:
        ! retryStrategy.canRetry(retriesSoFar, timeOfLastOperation, momentBeforeExpectedDelay)
        retryStrategy.canRetry(retriesSoFar, timeOfLastOperation, momentAfterExpectedDelay)

        where:
        retriesSoFar || howManyIntervalsShouldWait
        1            || 1
        2            || 1
        3            || 2
        4            || 3
        5            || 5
        6            || 8
        7            || 13
        8            || 21
        9            || 34
        10           || 55
        11           || 89
        12           || 144
        13           || 233
        14           || 233
        9999         || 233

        howManyMinutesShouldWait = howManyIntervalsShouldWait * baseIntervalInMinutes
        intervalInMinutes = baseIntervalInMinutes
    }

    DateTime someTime = DateTime.now()
    @Shared
    int baseIntervalInMinutes = 3
    Interval baseInterval = new Interval(3, TimeUnit.MINUTES)
    RetryStrategy retryStrategy = new FibonacciDelayingRetryStrategy(baseInterval)


}
