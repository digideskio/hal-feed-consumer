package com.qmetric.feed.consumer

import com.qmetric.hal.reader.HalReader
import com.qmetric.hal.reader.HalResource
import spock.lang.Specification

class DefaultResourceResolverTest extends Specification {

    def feedUrl = "http://localhost/feed"

    def endpointFactory = Mock(FeedEndpointFactory)

    def halReader = Mock(HalReader)

    def resolver = new DefaultResourceResolver(feedUrl, endpointFactory, halReader)

    def 'gets endpoint reader and passes it to the representation factory'()
    {
        given:
        def reader = Mock(Reader)
        def resource = Mock(HalResource)
        when:
        def result = resolver.resolve(EntryId.of("1"))
        then:
        1 * endpointFactory.create(_ as String) >> Mock(FeedEndpoint) { it.get() >> reader }
        1 * halReader.read(reader) >> resource
        result == resource
    }
}
