package com.qmetric.feed.consumer.metrics

import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Timer
import com.qmetric.feed.consumer.FeedConsumer
import com.qmetric.feed.consumer.FeedConsumer_
import spock.lang.Specification

class FeedConsumerWithMetricsTest extends Specification {

    final metricRegistry = Mock(MetricRegistry)

    final consumer = Mock(FeedConsumer_)

    final timer = Mock(Timer)

    final timerContext = Mock(Timer.Context)

    final errorMeter = Mock(Meter)

    final successMeter = Mock(Meter)

    final numberConsumedMeter = Mock(Meter)

    def policyConsumerWithMetrics

    def setup()
    {
        metricRegistry.register("feedPolling.timeTaken", _ as Timer) >> timer
        metricRegistry.meter("feedPolling.errors") >> errorMeter
        metricRegistry.meter("feedPolling.success") >> successMeter
        metricRegistry.meter("feedPolling.consumedEntries") >> numberConsumedMeter
        policyConsumerWithMetrics = new FeedConsumerWithMetrics(metricRegistry, consumer)
        timer.time() >> timerContext
    }

    def "should record time taken to poll feed for new entries"()
    {
        when:
        policyConsumerWithMetrics.consume()

        then:
        1 * timer.time() >> timerContext

        then:
        1 * consumer.consume() >> []

        then:
        1 * timerContext.stop()
    }

    def "should record each successful poll"()
    {
        when:
        policyConsumerWithMetrics.consume()

        then:
        1 * consumer.consume() >> []

        then:
        1 * successMeter.mark()
        0 * errorMeter.mark()
    }

    def "should record each unsuccessful poll"()
    {
        given:
        consumer.consume() >> { throw new Exception() }

        when:
        policyConsumerWithMetrics.consume()

        then:
        1 * errorMeter.mark()
        0 * successMeter.mark(_)
        thrown(Exception)
    }

    def "should record number of consumed entries for poll"()
    {
        when:
        policyConsumerWithMetrics.consume()

        then:
        1 * consumer.consume() >> []

        then:
        1 * numberConsumedMeter.mark(0)
    }
}
