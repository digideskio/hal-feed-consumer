package com.qmetric.feed.consumer

import spock.lang.Specification

import java.util.concurrent.TimeUnit

class IntervalTest extends Specification {

    def "should construct interval with valid parameters"()
    {
        when:
        final interval = new Interval(1, TimeUnit.MINUTES)

        then:
        interval
    }

    def "should throw exception if interval parameters is invalid"()
    {
        when:
        new Interval(0, TimeUnit.MINUTES)

        then:
        thrown(RuntimeException)
    }

    def "should create copy of itself multiplied by a given factor"() {
        given:
        Interval interval = new Interval(5, TimeUnit.MINUTES)
        long originalIntervalInMillis = interval.asMillis()

        when:
        Interval newInterval = interval.times(4)

        then:
        interval.asMillis() == originalIntervalInMillis
        newInterval.asMillis() == originalIntervalInMillis * 4
    }
}
