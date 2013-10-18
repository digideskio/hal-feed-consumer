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

class SimpleDBConsumedStoreTest extends Specification
{

    final feedHref = anyNonEmptyString()

    final domain = anyNonEmptyString()

    final feedEntry = new Link(Mock(RepresentationFactory), 'self', feedHref)

    final simpleDBClient = Mock(AmazonSimpleDB)

    final consumedEntryStore = new SimpleDBConsumedStore(simpleDBClient, domain)

    def "should store entry with consuming state only if not already consumed"()
    {
        when:
        consumedEntryStore.markAsConsuming(feedEntry)

        then:
        1 * simpleDBClient.putAttributes(_) >> { PutAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == feedHref
            assert r.attributes.size() == 1
            assert r.attributes.get(0).name == "consuming"
            assert r.expected.name == "consuming" && !r.expected.exists
        }
    }

    def "should throw AlreadyConsumingException when attempting to set consuming state for entry already being consumed by another consumer"()
    {
        given:
        simpleDBClient.putAttributes(_) >> {
            final error = new AmazonServiceException("Conditional check failed. Attribute (consuming) value exists")
            error.setErrorCode("ConditionalCheckFailed")
            throw error
        }

        when:
        consumedEntryStore.markAsConsuming(feedEntry)

        then:
        thrown(AlreadyConsumingException)
    }

    def "should revert entry as being in consuming state"()
    {
        when:
        consumedEntryStore.revertConsuming(feedEntry)

        then:
        1 * simpleDBClient.deleteAttributes(_) >> { DeleteAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == feedHref
            assert r.attributes.size() == 1
            assert r.attributes.get(0).getName() == "consuming"
        }
    }

    def "should store entry with consumed state"()
    {
        when:
        consumedEntryStore.markAsConsumed(feedEntry)

        then:
        simpleDBClient.putAttributes(_) >> { PutAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == feedHref
            assert r.attributes.size() == 1
            assert r.attributes.get(0).getName() == "consumed"
        }
    }

    def "should return whether entry has already been consumed"()
    {
        when:
        final notConsumedResult = consumedEntryStore.notAlreadyConsumed(feedEntry)

        then:
        1 * simpleDBClient.select(_) >> new SelectResult().withItems(new Item())
        !notConsumedResult
    }

    def "should return whether entry has not yet been consumed"()
    {
        when:
        final notConsumedResult = consumedEntryStore.notAlreadyConsumed(feedEntry)

        then:
        1 * simpleDBClient.select(_) >> new SelectResult()
        notConsumedResult
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
}