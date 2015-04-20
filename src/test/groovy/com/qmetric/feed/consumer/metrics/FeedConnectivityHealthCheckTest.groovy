package com.qmetric.feed.consumer.metrics

import com.theoryinpractise.halbuilder.api.RepresentationFactory
import org.glassfish.jersey.client.ClientResponse
import spock.lang.Specification

import javax.ws.rs.client.Client
import javax.ws.rs.client.Invocation
import javax.ws.rs.client.WebTarget

class FeedConnectivityHealthCheckTest extends Specification {

    final client = Mock(Client)

    final webTarget = Mock(WebTarget)

    final invocationBuilder = Mock(Invocation.Builder)

    final response = Mock(ClientResponse)

    final healthCheck = new FeedConnectivityHealthCheck("http://host:123/", client)

    def setup()
    {
        client.target("http://host:123/ping") >> webTarget
        webTarget.request(RepresentationFactory.HAL_JSON) >> invocationBuilder
        invocationBuilder.get(ClientResponse.class) >> response
    }

    def "should know when feed connectivity is healthy"()
    {
        given:
        response.getStatus() >> 200

        when:
        final result = healthCheck.check()

        then:
        result.isHealthy()
    }

    def "should know when feed connectivity is unhealthy"()
    {
        given:
        response.getStatus() >> 500

        when:
        final result = healthCheck.check()

        then:
        !result.isHealthy()
    }
}
