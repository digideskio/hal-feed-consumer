package com.qmetric.feed.consumer.metrics

import com.squareup.okhttp.mockwebserver.MockResponse
import com.squareup.okhttp.mockwebserver.rule.MockWebServerRule
import org.apache.http.HttpHeaders
import org.junit.ClassRule
import spock.lang.Shared
import spock.lang.Specification

import javax.ws.rs.client.ClientBuilder
import javax.ws.rs.core.MediaType

class FeedConnectivityHealthCheckTest extends Specification {

    @ClassRule @Shared
    MockWebServerRule server = new MockWebServerRule()

    def healthCheck

    def response = new MockResponse().addHeader(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_PLAIN)

    def setup()
    {
        healthCheck = new FeedConnectivityHealthCheck(
                server.getUrl("/ping").toString(),
                ClientBuilder.newClient())
    }

    def "should know when feed connectivity is healthy"()
    {
        given:
        server.enqueue(response.setResponseCode(200)
                .setBody("pong"))

        when:
        final result = healthCheck.check()

        then:
        result.isHealthy()
    }

    def "should know when feed connectivity is unhealthy"()
    {
        given:
        server.enqueue(response.setResponseCode(500)
                .setBody("error"))

        when:
        final result = healthCheck.check()

        then:
        !result.isHealthy()
    }
}
