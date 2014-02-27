package com.qmetric.feed.consumer.metrics

import com.qmetric.feed.consumer.store.ConnectivityException
import com.qmetric.feed.consumer.store.FeedTracker
import spock.lang.Specification

class FeedTrackerConnectivityHealthCheckTest extends Specification {

    final feedTracker = Mock(FeedTracker)

    final simpleDBHealthCheck = new FeedTrackerConnectivityHealthCheck(feedTracker)

    def "should know when feed tracker store connectivity is healthy"()
    {
        when:
        final result = simpleDBHealthCheck.check()

        then:
        result.isHealthy()
    }

    def "should know when feed tracker store connectivity is unhealthy"()
    {
        given:
        feedTracker.checkConnectivity() >> { throw new ConnectivityException(new Exception()) }

        when:
        final result = simpleDBHealthCheck.check()

        then:
        !result.isHealthy()
    }
}
