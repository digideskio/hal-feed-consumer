package com.qmetric.feed.consumer.metrics

import com.qmetric.feed.consumer.store.ConnectivityException
import com.qmetric.feed.consumer.store.FeedTracker
import spock.lang.Specification

class ConsumedStoreConnectivityHealthCheckTest extends Specification {

    final consumedStore = Mock(FeedTracker)

    final simpleDBHealthCheck = new ConsumedStoreConnectivityHealthCheck(consumedStore)

    def "should know when consumed store connectivity is healthy"()
    {
        when:
        final result = simpleDBHealthCheck.check()

        then:
        result.isHealthy()
    }

    def "should know when consumed store connectivity is unhealthy"()
    {
        given:
        consumedStore.checkConnectivity() >> { throw new ConnectivityException(new Exception()) }

        when:
        final result = simpleDBHealthCheck.check()

        then:
        !result.isHealthy()
    }
}
