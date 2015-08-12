package com.qmetric.feed.consumer.metrics

import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.ProtocolVersion
import org.apache.http.StatusLine
import org.apache.http.client.HttpClient
import org.apache.http.message.BasicStatusLine
import spock.lang.Specification

class FeedConnectivityHealthCheckTest extends Specification {

    final String url = "http://www.example.com/ping"

    def "should know when feed connectivity is healthy"()
    {
        given:
        def healthCheck = getMockHealthCheck(200, "OK", "pong")

        when:
        final result = healthCheck.check()

        then:
        result.isHealthy()
    }

    def "should know when feed connectivity is unhealthy"()
    {
        given:
        def healthCheck = getMockHealthCheck(500, "Server Error", "error")

        when:
        final result = healthCheck.check()

        then:
        !result.isHealthy()
    }

    def getMockHealthCheck(final int statusCode, final String status, final String response)
    {
        return new FeedConnectivityHealthCheck(url, getMockHttpClient(statusCode, status, response))
    }

    def getMockHttpClient(final int statusCode, final String status, final String response)
    {
        def httpClient = Mock(HttpClient)
        def httpResponse = Mock(HttpResponse)
        def httpEntity = Mock(HttpEntity)
        def responseStream = new ByteArrayInputStream(response.getBytes())

        httpClient.execute(_) >> httpResponse
        httpResponse.getStatusLine() >> getStatus(statusCode, status)
        httpResponse.getEntity() >> httpEntity
        httpEntity.getContent() >> responseStream

        return httpClient
    }

    private static StatusLine getStatus(int statusCode, String status)
    {
        new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), statusCode, status)
    }
}
