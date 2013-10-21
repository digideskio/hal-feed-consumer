package com.qmetric.feed.consumer

import spock.lang.Specification

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

class FeedConsumerSchedulerTest extends Specification
{

    final interval = new Interval(1, TimeUnit.MINUTES)

    final schedulerExecutionService = Mock(ScheduledExecutorService)

    final consumer = Mock(FeedConsumerImpl)

    final scheduler = new FeedConsumerScheduler(consumer, interval, schedulerExecutionService)

    def "should periodically consume feed"()
    {
        when:
        scheduler.start()

        then:
        1 * schedulerExecutionService.scheduleAtFixedRate(_, 0, interval.time, interval.unit)
    }

    def "should catch any exception when consuming feed"()
    {
        given:
        consumer.consume() >> { throw new Exception() }

        //noinspection GroovyAccessibility
        when:
        scheduler.consume()

        then:
        notThrown(Exception)
    }
}
