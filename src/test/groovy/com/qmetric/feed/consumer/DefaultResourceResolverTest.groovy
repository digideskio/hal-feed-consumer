package com.qmetric.feed.consumer

import com.theoryinpractise.halbuilder.api.ReadableRepresentation
import com.theoryinpractise.halbuilder.api.RepresentationFactory
import spock.lang.Specification

class DefaultResourceResolverTest extends Specification {

    def feedUrl = "http://localhost/feed"

    def endpointFactory = Mock(FeedEndpointFactory)

    def representationFactory = Mock(RepresentationFactory)

    def resolver = new DefaultResourceResolver(feedUrl, endpointFactory, representationFactory)

    def 'gets endpoint reader and passes it to the representation factory'()
    {
        given:
        def reader = Mock(Reader)
        def representation = Mock(ReadableRepresentation)
        when:
        def result = resolver.resolve(EntryId.of("1"))
        then:
        1 * endpointFactory.create(_ as String) >> Mock(FeedEndpoint) { it.get() >> reader }
        1 * representationFactory.readRepresentation(reader) >> representation
        result == representation
    }
}
