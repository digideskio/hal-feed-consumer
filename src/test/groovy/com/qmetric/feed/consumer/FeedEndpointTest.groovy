package com.qmetric.feed.consumer

import spock.lang.Specification

import javax.ws.rs.client.Invocation
import javax.ws.rs.client.WebTarget
import javax.ws.rs.core.Response

import static javax.ws.rs.core.Response.Status.NOT_FOUND
import static javax.ws.rs.core.Response.Status.OK
import static net.java.quickcheck.generator.PrimitiveGeneratorSamples.anyNonEmptyString
import static org.apache.commons.io.IOUtils.toString

class FeedEndpointTest extends Specification {
    def "should send request to endpoint and return response as io reader"()
    {
        given:
        def expectedString = anyNonEmptyString()
        def inputStream = new ByteArrayInputStream(expectedString.getBytes("UTF-8"))
        def target = Mock(WebTarget)
        def invocationBuilder = Mock(Invocation.Builder)
        def response = Mock(Response)
        def feedEndpoint = new FeedEndpoint(target)
        response.getStatus() >> OK.statusCode

        when:
        final reader = feedEndpoint.get()

        then:
        1 * target.request("application/hal+json") >> invocationBuilder
        1 * invocationBuilder.get() >> response
        1 * response.readEntity(InputStream) >> inputStream
        expectedString == toString(reader.get())
    }

    def 'should return nothing when feed entry does not exist'()
    {
        given:
        def target = Mock(WebTarget) { t ->
            t.request(_) >> Mock(Invocation.Builder) { Invocation.Builder b ->
                b.get() >> Mock(Response) { Response r -> //
                    r.getStatus() >> NOT_FOUND.statusCode
                    r.readEntity(String.class) >> "Feed entry not found" }
            }
        }
        def endpoint = new FeedEndpoint(target)

        expect:
        !endpoint.get().isPresent()
    }

    def 'throws exception when http request fails'()
    {
        given:
        def target = Mock(WebTarget) { t ->
            t.request(_) >> Mock(Invocation.Builder) { Invocation.Builder b ->
                b.get() >> Mock(Response) { Response r -> //
                    r.getStatus() >> NOT_FOUND.statusCode }
            }
        }
        def endpoint = new FeedEndpoint(target)

        when:
        endpoint.get()

        then:
        thrown(IllegalStateException)
    }
}
