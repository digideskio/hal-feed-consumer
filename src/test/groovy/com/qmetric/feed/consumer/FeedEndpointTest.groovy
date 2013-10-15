package com.qmetric.feed.consumer

import com.sun.jersey.api.client.ClientResponse
import com.sun.jersey.api.client.WebResource
import spock.lang.Specification

import static com.sun.jersey.api.client.ClientResponse.Status.NOT_FOUND
import static com.sun.jersey.api.client.ClientResponse.Status.OK
import static net.java.quickcheck.generator.PrimitiveGeneratorSamples.anyNonEmptyString
import static org.apache.commons.io.IOUtils.toString

class FeedEndpointTest extends Specification
{
    def "should send request to endpoint and return response as io reader"()
    {
        given:
        def expectedString = anyNonEmptyString()
        def inputStream = new ByteArrayInputStream(expectedString.getBytes("UTF-8"))
        def resource = Mock(WebResource)
        def resourceBuilder = Mock(WebResource.Builder)
        def response = Mock(ClientResponse)
        def feedEndpoint = new FeedEndpoint(resource)

        when:
        final reader = feedEndpoint.get()

        then:
        1 * resource.accept("application/hal+json") >> resourceBuilder
        1 * resourceBuilder.get(ClientResponse.class) >> response
        1 * response.getEntityInputStream() >> inputStream
        1 * response.getClientResponseStatus() >> OK
        expectedString == toString(reader)
    }

    def 'throws exception when http request fails'()
    {
        given:
        def resource = Mock(WebResource) { r ->
            r.accept(_) >> Mock(WebResource.Builder) { WebResource.Builder b ->
                b.get(ClientResponse) >> Mock(ClientResponse) { ClientResponse c -> //
                    c.getClientResponseStatus() >> NOT_FOUND }
            }
        }
        def endpoint = new FeedEndpoint(resource)

        when:
        endpoint.get()
        then:
        def e = thrown(IllegalStateException)
        e.message.contains(NOT_FOUND.reasonPhrase)
        e.message.contains(NOT_FOUND.statusCode as String)
    }
}
