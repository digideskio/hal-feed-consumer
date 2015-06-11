package com.qmetric.feed.consumer.store

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.simpledb.AmazonSimpleDB
import com.amazonaws.services.simpledb.model.*
import com.qmetric.feed.consumer.DateTimeSource
import com.qmetric.feed.consumer.EntryId
import com.qmetric.feed.consumer.SeenEntry
import com.qmetric.feed.consumer.TrackedEntry
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

    private static final CURRENT_DATE_STRING = "2014/01/10 12:00:00"

    private static final CURRENT_DATE = new DateTime(2014, 1, 10, 12, 0, 0, 0)

    final domain = anyNonEmptyString()

    final feedEntryId = EntryId.of(anyNonEmptyString())

    final simpleDBClient = Mock(AmazonSimpleDB)

    final dateTimeSource = Mock(DateTimeSource.class)

    final consumedEntryStore = new SimpleDBFeedTracker(simpleDBClient, domain, dateTimeSource)

    def setup() {
        dateTimeSource.now() >> CURRENT_DATE
    }

    def "should store entry with consuming state only if not already consuming"()
    {
        when:
        consumedEntryStore.markAsConsuming(feedEntryId)

        then:
        1 * simpleDBClient.putAttributes(_) >> { PutAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == itemName(feedEntryId)
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
        consumedEntryStore.markAsConsuming(feedEntryId)

        then:
        1 * simpleDBClient.putAttributes(_) >> {
            final error = new AmazonServiceException("Conditional check failed. Attribute (consuming) value exists")
            error.setErrorCode("ConditionalCheckFailed")
            throw error
        }
        thrown(AlreadyConsumingException)
    }

    def 'fail should increment fail count and remove "consuming" attribute'()
    {
        when:
        consumedEntryStore.fail(new TrackedEntry(feedEntryId, CURRENT_DATE, initial_count), true)

        then: 'increment failures count and update seen_at date'
        1 * simpleDBClient.putAttributes(_ as PutAttributesRequest) >> { PutAttributesRequest it ->
            assert it.domainName == domain
            assert it.itemName == feedEntryId.toString()
            assert it.attributes.size() == 2
            assert it.attributes.contains(expectedFailureCountAttribute)
            assert it.attributes.contains(expectedSeenAtAttribute)
        }

        and: 'revert consuming'
        1 * simpleDBClient.deleteAttributes(_ as DeleteAttributesRequest) >> { DeleteAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == feedEntryId.toString()
            assert r.attributes.size() == 1
            assert r.attributes.get(0).getName() == CONSUMING
        }

        where:
        initial_count | incremented_count
        0             | 1
        1             | 2
        2             | 3

        expectedFailureCountAttribute = new ReplaceableAttribute(FAILURES_COUNT, incremented_count.toString(), true)
        expectedSeenAtAttribute = new ReplaceableAttribute(SEEN_AT, CURRENT_DATE_STRING, true)
        initialFailureCountAttribute = initial_count == null ? null : new Attribute(FAILURES_COUNT, initial_count.toString())
    }

    def 'fail should abort further retries if max retries exceeded'()
    {
        given:
        final expectedAbortedAttribute = new ReplaceableAttribute("aborted", CURRENT_DATE_STRING, true)

        when:
        consumedEntryStore.fail(new TrackedEntry(feedEntryId, CURRENT_DATE, 0), false)

        then: 'increment failures count and update seen_at date'
        1 * simpleDBClient.putAttributes(_ as PutAttributesRequest) >> { PutAttributesRequest it ->
            assert it.domainName == domain
            assert it.itemName == feedEntryId.toString()
            assert it.attributes.size() == 1
            assert it.attributes.contains(expectedAbortedAttribute)
        }

        and: 'revert consuming'
        1 * simpleDBClient.deleteAttributes(_ as DeleteAttributesRequest) >> { DeleteAttributesRequest r ->
            assert r.attributes.get(0).getName() == CONSUMING
        }
    }

    def 'should mark consumed entry with "consumed" attribute'()
    {
        when:
        consumedEntryStore.markAsConsumed(feedEntryId)

        then:
        simpleDBClient.putAttributes(_) >> { PutAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == feedEntryId.toString()
            assert r.attributes.size() == 1
            def attribute = r.attributes.get(0)
            assert attribute.name == "consumed"
            assert !isBlank(attribute.value)
        }
    }

    def 'should add expected attributes when tracking'()
    {
        given:
        final dateTime = new DateTime(2015, 5, 1, 12, 0, 0, 0)

        when:
        consumedEntryStore.track(new SeenEntry(feedEntryId, dateTime))

        then:
        1 * simpleDBClient.putAttributes(_ as PutAttributesRequest) >> { PutAttributesRequest r ->
            assert r.itemName == feedEntryId.toString()
            assert r.attributes.get(0) == new ReplaceableAttribute("created", "2015/05/01 12:00:00", false)
            assert r.attributes.get(1) == new ReplaceableAttribute("seen_at", "2015/05/01 12:00:00", true)
        }
    }

    def 'should return whether entry is tracked or not'()
    {
        when:
        final isTracked = consumedEntryStore.isTracked(feedEntryId)

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
        def notConsumedResult = consumedEntryStore.getEntriesToBeConsumed()

        then:
        1 * simpleDBClient.select(_) >> { SelectRequest r ->
            def whereCondition = r.getSelectExpression().split("(?i)where")[1]
            assert whereCondition.contains('aborted')
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