package com.qmetric.feed.consumer.metrics

import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Timer
import com.qmetric.feed.consumer.EntryConsumer
import com.theoryinpractise.halbuilder.api.ReadableRepresentation
import spock.lang.Specification

class EntryConsumerWithMetricsTest extends Specification {

    final metricRegistry = Mock(MetricRegistry)

    final entryConsumer = Mock(EntryConsumer)

    final feedEntry = Mock(ReadableRepresentation)

    final timer = Mock(Timer)

    final timerContext = Mock(Timer.Context)

    final errorMeter = Mock(Meter)

    final successMeter = Mock(Meter)

    def entryConsumerWithMetrics

    def setup()
    {
        metricRegistry.register("entryConsumption.timeTaken", _ as Timer) >> timer
        metricRegistry.meter("entryConsumption.errors") >> errorMeter
        metricRegistry.meter("entryConsumption.success") >> successMeter
        entryConsumerWithMetrics = new EntryConsumerWithMetrics(metricRegistry, entryConsumer)
        timer.time() >> timerContext
    }

    def "should record time taken to consume feed entry"()
    {
        when:
        entryConsumerWithMetrics.consume(feedEntry)

        then:
        1 * timer.time() >> timerContext

        then:
        1 * entryConsumer.consume(feedEntry)

        then:
        1 * timerContext.stop()
    }

    def "should record each successful consumption"()
    {
        when:
        entryConsumerWithMetrics.consume(feedEntry)

        then:
        1 * entryConsumer.consume(feedEntry)

        then:
        1 * successMeter.mark()
        0 * errorMeter.mark()
    }

    def "should record each unsuccessful consumption"()
    {
        given:
        entryConsumer.consume(feedEntry) >> { throw new Exception() }

        when:
        entryConsumerWithMetrics.consume(feedEntry)

        then:
        1 * errorMeter.mark()
        0 * successMeter.mark()
        thrown(Exception)
    }
}
