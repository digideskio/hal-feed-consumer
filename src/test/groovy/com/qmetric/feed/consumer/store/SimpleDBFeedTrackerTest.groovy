package com.qmetric.feed.consumer.store

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.simpledb.AmazonSimpleDB
import com.amazonaws.services.simpledb.model.*
import com.theoryinpractise.halbuilder.api.Link
import com.theoryinpractise.halbuilder.api.RepresentationFactory
import spock.lang.Specification

import static com.google.common.collect.Iterables.size
import static net.java.quickcheck.generator.PrimitiveGeneratorSamples.anyNonEmptyString
import static net.java.quickcheck.generator.PrimitiveGeneratorsIterables.someObjects
import static org.apache.commons.lang3.StringUtils.isBlank

class SimpleDBFeedTrackerTest extends Specification
{

    final domain = anyNonEmptyString()

    final feedEntry = new Link(Mock(RepresentationFactory), anyNonEmptyString(), anyNonEmptyString())

    final simpleDBClient = Mock(AmazonSimpleDB)

    final consumedEntryStore = new SimpleDBFeedTracker(simpleDBClient, domain)

    def "should store entry with consuming state only if not already consuming"()
    {
        when:
        consumedEntryStore.markAsConsuming(feedEntry)

        then:
        1 * simpleDBClient.putAttributes(_) >> { PutAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == itemName(feedEntry)
            assert r.attributes.size() == 1
            def attribute = r.attributes.get(0)
            assert attribute.name == "consuming"
            assert !isBlank(attribute.value)
            assert r.expected.name == "consuming"
            assert r.expected.exists == false
        }
    }

    def "should throw AlreadyConsumingException when attempting to set consuming state for entry already being consumed by another consumer"()
    {
        when:
        consumedEntryStore.markAsConsuming(feedEntry)

        then:
        1 * simpleDBClient.putAttributes(_) >> {
            final error = new AmazonServiceException("Conditional check failed. Attribute (consuming) value exists")
            error.setErrorCode("ConditionalCheckFailed")
            throw error
        }
        thrown(AlreadyConsumingException)
    }

    def 'revert consuming should remove "consuming" attribute'()
    {
        when:
        consumedEntryStore.revertConsuming(feedEntry)

        then:
        1 * simpleDBClient.deleteAttributes(_) >> { DeleteAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == feedEntry.href
            assert r.attributes.size() == 1
            assert r.attributes.get(0).getName() == "consuming"
        }
    }

    def 'should mark consumed entry with "consumed" attribute'()
    {
        when:
        consumedEntryStore.markAsConsumed(feedEntry)

        then:
        simpleDBClient.putAttributes(_) >> { PutAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == feedEntry.href
            assert r.attributes.size() == 1
            def attribute = r.attributes.get(0)
            assert attribute.name == "consumed"
            assert !isBlank(attribute.value)
        }
    }

    def 'should add new entry with "seen_at" attribute'()
    {
        when:
        consumedEntryStore.track(feedEntry)

        then:
        1 * simpleDBClient.putAttributes(_ as PutAttributesRequest) >> { PutAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == feedEntry.href
            assert r.attributes.size() == 1
            def attribute = r.attributes.get(0)
            assert attribute.name == "seen_at"
            assert !isBlank(attribute.value)
        }

        where:
        returnedItems | expected
        []            | false
        [new Item()]  | true
    }

    def 'should return whether entry is tracked or not'()
    {
        when:
        final isTracked = consumedEntryStore.isTracked(feedEntry)

        then:
        1 * simpleDBClient.select(_ as SelectRequest) >> new SelectResult().withItems(returnedItems)
        isTracked == expected

        where:
        returnedItems | expected
        []            | false
        [new Item()]  | true
    }

    def "should return list of unconsumed entries"()
    {
        given:
        def items = someItems()

        when:
        def notConsumedResult = consumedEntryStore.getItemsToBeConsumed()

        then:
        1 * simpleDBClient.select(_) >> new SelectResult().withItems(items)
        size(notConsumedResult) == size(items)
    }

    def "should know when connectivity to store is healthy"()
    {
        when:
        consumedEntryStore.checkConnectivity()

        then:
        notThrown(ConnectivityException)
    }

    def "should know when connectivity to store is unhealthy"()
    {
        when:
        consumedEntryStore.checkConnectivity()

        then:
        1 * simpleDBClient.domainMetadata(new DomainMetadataRequest(domain)) >> { throw new Exception() }
        thrown(ConnectivityException)
    }

    private static List<Item> someItems()
    {
        someObjects().collect { new Item() }
    }

    private static String itemName(final Link entry)
    {
        entry.href
    }
}