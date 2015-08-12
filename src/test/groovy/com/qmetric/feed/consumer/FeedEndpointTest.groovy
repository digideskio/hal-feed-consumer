package com.qmetric.feed.consumer

import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.ProtocolVersion
import org.apache.http.StatusLine
import org.apache.http.client.HttpClient
import org.apache.http.message.BasicStatusLine
import spock.lang.Specification

import static net.java.quickcheck.generator.PrimitiveGeneratorSamples.anyNonEmptyString
import static org.apache.commons.io.IOUtils.toString

class FeedEndpointTest extends Specification
{
    final String url = "url"

    def "should send request to endpoint and return response as io reader"()
    {
        given:
        def expectedString = anyNonEmptyString()
        def httpClient = getMockHttpClient(expectedString)
        def feedEndpoint = new FeedEndpoint(httpClient, url)

        when:
        final reader = feedEndpoint.get()

        then:
        expectedString == toString(reader.get())
    }

    def 'should return nothing when feed entry does not exist'()
    {
        given:
        def httpClient = getMockHttpClient(404, "NOT FOUND", "Feed entry not found")
        def feedEndpoint = new FeedEndpoint(httpClient, url)

        expect:
        !feedEndpoint.get().isPresent()
    }

    def 'throws exception when http request fails'()
    {
        given:
        def httpClient = getMockHttpClient(404, "NOT FOUND", "Another random error")
        def feedEndpoint = new FeedEndpoint(httpClient, url)

        when:
        feedEndpoint.get()

        then:
        thrown(IllegalStateException)
    }

    def getMockHttpClient(final String response)
    {
        return getMockHttpClient(200, "OK", response)
    }

    def getMockHttpClient(final int statusCode, final String status)
    {
        return getMockHttpClient(statusCode, status, null)
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
