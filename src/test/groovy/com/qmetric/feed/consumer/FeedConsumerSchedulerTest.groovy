package com.qmetric.feed.consumer

import spock.lang.Specification

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

class FeedConsumerSchedulerTest extends Specification {

    final interval = new Interval(1, TimeUnit.MINUTES)

    final scheduledExecutionService = Mock(ScheduledExecutorService)

    final consumer = Mock(FeedConsumerImpl)
    final tracker = Mock(AvailableFeedEntriesTracker)

    final scheduler = new FeedConsumerScheduler(consumer, tracker, interval, scheduledExecutionService, true, "http://localhost/feed")

    def "should periodically consume feed"()
    {
        when:
        scheduler.start()

        then:
        1 * scheduledExecutionService.scheduleAtFixedRate(_ as Runnable, 0, interval.time, interval.unit)
    }

    def "should catch any exception when consuming feed"()
    {
        when:
        //noinspection GroovyAccessibility
        scheduler.consume()

        then:
        1 * consumer.consume() >> { throw new Exception() }
        notThrown(Exception)
    }

    def "should catch any exception when updating feed-tracker"()
    {
        when:
        //noinspection GroovyAccessibility
        scheduler.updateTracker()

        then:
        1 * tracker.trackNewEntries() >> { throw new Exception() }
        notThrown(Exception)
    }
}
