package com.qmetric.feed.consumer.metrics

import com.qmetric.feed.consumer.EntryId
import com.qmetric.feed.consumer.Interval
import org.joda.time.DateTime
import spock.lang.Specification
import spock.lang.Unroll

import static java.util.concurrent.TimeUnit.MINUTES
import static java.util.concurrent.TimeUnit.SECONDS
import static net.java.quickcheck.generator.PrimitiveGeneratorSamples.anyString

class PollingActivityHealthCheckTest extends Specification {

    final dateTimeSource = Mock(PollingActivityHealthCheck.DateTimeSource)

    def "should be unhealthy if never any polling activity"()
    {
        given:
        final healthCheck = new PollingActivityHealthCheck(new Interval(1, MINUTES))

        when:
        final result = healthCheck.check()

        then:
        !result.isHealthy()
    }

    @Unroll def "should evaluate health of feed polling using date of last activity"()
    {
        given:
        final healthCheck = new PollingActivityHealthCheck(interval, dateTimeSource)
        dateTimeSource.now() >>> [lastConsumedDate, currentDate]

        when:
        healthCheck.consumed(EntryId.of(anyString()))

        then:
        healthCheck.check().isHealthy() == expectedHealthyResult

        where:
        interval                 | lastConsumedDate                      | currentDate                             | expectedHealthyResult
        new Interval(1, MINUTES) | new DateTime(2013, 7, 19, 0, 0, 0, 0) | new DateTime(2013, 7, 19, 0, 1, 0, 0)   | true
        new Interval(1, MINUTES) | new DateTime(2013, 7, 19, 0, 0, 0, 0) | new DateTime(2013, 7, 19, 0, 0, 59, 59) | true
        new Interval(1, MINUTES) | new DateTime(2013, 7, 19, 0, 0, 0, 0) | new DateTime(2013, 7, 19, 0, 1, 0, 1)   | false
        new Interval(1, SECONDS) | new DateTime(2013, 7, 19, 0, 0, 0, 0) | new DateTime(2013, 7, 19, 0, 0, 1, 0)   | true
        new Interval(1, SECONDS) | new DateTime(2013, 7, 19, 0, 0, 0, 0) | new DateTime(2013, 7, 19, 0, 0, 2, 0)   | false
    }


}
