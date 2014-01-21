package com.qmetric.feed.consumer.store
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.simpledb.AmazonSimpleDB
import com.amazonaws.services.simpledb.model.*
import com.qmetric.feed.consumer.DateTimeSource
import com.qmetric.feed.consumer.EntryId
import org.joda.time.DateTime
import spock.lang.Specification

import static com.google.common.collect.Iterables.size
import static net.java.quickcheck.generator.PrimitiveGeneratorSamples.anyNonEmptyString
import static net.java.quickcheck.generator.PrimitiveGeneratorsIterables.someObjects
import static org.apache.commons.lang3.StringUtils.isBlank

class SimpleDBFeedTrackerTest extends Specification {

    private static FAILURES_COUNT = 'failures_count'

    private static SEEN_AT = 'seen_at'

    private static CONSUMING = "consuming"

    final domain = anyNonEmptyString()

    final feedEntry = EntryId.of(anyNonEmptyString())

    final simpleDBClient = Mock(AmazonSimpleDB)

    final dateTimeSource = Mock(DateTimeSource.class)

    final consumedEntryStore = new SimpleDBFeedTracker(simpleDBClient, domain, dateTimeSource)

    def setup() {
        dateTimeSource.now() >> new DateTime(2014, 1, 10, 12, 0, 0, 0)
    }

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
            assert attribute.name == CONSUMING
            assert !isBlank(attribute.value)
            assert r.expected.name == CONSUMING
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
            assert r.itemName == feedEntry.toString()
            assert r.attributes.size() == 1
            assert r.attributes.get(0).getName() == CONSUMING
        }
    }

    def 'fail should increment fail count and remove "consuming" attribute'()
    {
        when:
        consumedEntryStore.fail(feedEntry)

        then: 'get current failures count'
        1 * simpleDBClient.getAttributes(_ as GetAttributesRequest) >> { GetAttributesRequest it ->
            assert it.domainName == domain
            assert it.itemName == feedEntry.toString()
            assert it.attributeNames == [FAILURES_COUNT]
            return new GetAttributesResult().withAttributes(initialFailureCountAttribute)
        }

        and: 'increment failures count and update seen_at date'
        1 * simpleDBClient.putAttributes(_ as PutAttributesRequest) >> { PutAttributesRequest it ->
            assert it.domainName == domain
            assert it.itemName == feedEntry.toString()
            assert it.attributes.size() == 2
            assert it.attributes.contains(expectedFailureCountAttribute)
            assert it.attributes.contains(expectedSeenAtAttribute)
        }

        and: 'revert consuming'
        1 * simpleDBClient.deleteAttributes(_ as DeleteAttributesRequest) >> { DeleteAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == feedEntry.toString()
            assert r.attributes.size() == 1
            assert r.attributes.get(0).getName() == CONSUMING
        }

        where:
        initial_count | incremented_count
        null          | '001'
        '001'         | '002'
        '002'         | '003'

        expectedFailureCountAttribute = new ReplaceableAttribute(FAILURES_COUNT, incremented_count, true)
        expectedSeenAtAttribute = new ReplaceableAttribute(SEEN_AT, "2014/01/10 12:00:00", true)
        initialFailureCountAttribute = initial_count == null ? null : new Attribute(FAILURES_COUNT, initial_count)
    }

    def 'should mark consumed entry with "consumed" attribute'()
    {
        when:
        consumedEntryStore.markAsConsumed(feedEntry)

        then:
        simpleDBClient.putAttributes(_) >> { PutAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == feedEntry.toString()
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
            assert r.itemName == feedEntry.toString()
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
        1 * simpleDBClient.select(_) >> { SelectRequest r ->
            def whereCondition = r.getSelectExpression().split("(?i)where")[1]
            assert whereCondition.contains('failures_count')
            assert whereCondition.contains('consuming')
            assert whereCondition.contains('consumed')
            return new SelectResult().withItems(items)
        }
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

    private static String itemName(final EntryId entry)
    {
        entry.toString()
    }
}