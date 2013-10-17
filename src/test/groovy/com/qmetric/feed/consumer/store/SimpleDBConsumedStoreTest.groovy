package com.qmetric.feed.consumer.store

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.simpledb.AmazonSimpleDB
import com.amazonaws.services.simpledb.model.*
import com.theoryinpractise.halbuilder.api.ReadableRepresentation
import spock.lang.Specification

import static com.google.common.collect.Iterables.size
import static net.java.quickcheck.generator.PrimitiveGeneratorsIterables.someObjects

class SimpleDBConsumedStoreTest extends Specification
{

    final feedEntryId = "1"

    final domain = "domain"

    final feedEntry = Mock(ReadableRepresentation)

    final selectResult = Mock(SelectResult)

    final simpleDBClient = Mock(AmazonSimpleDB)

    final consumedEntryStore = new SimpleDBConsumedStore(simpleDBClient, domain)

    def "should store entry with consuming state only if not already consumed"()
    {
        given:
        feedEntry.getValue("_id") >> feedEntryId

        when:
        consumedEntryStore.markAsConsuming(feedEntry)

        then:
        simpleDBClient.putAttributes(_) >> {
            final request = (it[0] as PutAttributesRequest)

            assert request.domainName == domain
            assert request.itemName == feedEntryId
            assert request.attributes.size() == 1
            assert request.attributes.get(0).getName() == "consuming"
            assert request.expected.name == "consuming" && !request.expected.exists
        }
    }

    def "should throw AlreadyConsumingException when attempting to set consuming state for entry already being consumed by another consumer"()
    {
        given:
        feedEntry.getValue("_id") >> feedEntryId
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
        given:
        feedEntry.getValue("_id") >> feedEntryId

        when:
        consumedEntryStore.revertConsuming(feedEntry)

        then:
        simpleDBClient.deleteAttributes(_) >> {
            final request = (it[0] as DeleteAttributesRequest)
            assert request.domainName == domain
            assert request.itemName == feedEntryId
            assert request.attributes.size() == 1
            assert request.attributes.get(0).getName() == "consuming"
        }
    }

    def "should store entry with consumed state"()
    {
        given:
        feedEntry.getValue("_id") >> feedEntryId

        when:
        consumedEntryStore.markAsConsumed(feedEntry)

        then:
        simpleDBClient.putAttributes(_) >> {
            final request = (it[0] as PutAttributesRequest)

            assert request.domainName == domain
            assert request.itemName == feedEntryId
            assert request.attributes.size() == 1
            assert request.attributes.get(0).getName() == "consumed"
        }
    }

    def "should return whether entry has already been consumed"()
    {
        given:
        selectResult.getItems() >> [new Item()]
        simpleDBClient.select(_) >> selectResult

        when:
        final notConsumedResult = consumedEntryStore.notAlreadyConsumed(feedEntry)

        then:
        !notConsumedResult
    }

    def "should return whether entry has not yet been consumed"()
    {
        given:
        selectResult.getItems() >> []
        simpleDBClient.select(_) >> selectResult

        when:
        final notConsumedResult = consumedEntryStore.notAlreadyConsumed(feedEntry)

        then:
        notConsumedResult
    }

    def "should return list of unconsumed entries"()
    {
        given:
        def items = someItems()
        selectResult.getItems() >> items
        simpleDBClient.select(_) >> selectResult

        when:
        final notConsumedResult = consumedEntryStore.getItemsToBeConsumed()

        then:
        size(notConsumedResult) == size(items)
    }

    private static List<Item> someItems()
    {
        someObjects().collect { new Item() }
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
        given:
        simpleDBClient.domainMetadata(new DomainMetadataRequest(domain)) >> { throw new Exception() }

        when:
        consumedEntryStore.checkConnectivity()

        then:
        thrown(ConnectivityException)
    }
}