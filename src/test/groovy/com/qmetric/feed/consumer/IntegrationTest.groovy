package com.qmetric.feed.consumer
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.amazonaws.services.simpledb.model.DeleteDomainRequest
import com.amazonaws.services.simpledb.model.DomainMetadataRequest
import com.amazonaws.services.simpledb.model.SelectRequest
import com.qmetric.feed.consumer.multipleClientsTest.MockEntryHandler
import com.qmetric.feed.consumer.multipleClientsTest.MockFeedHandler
import com.qmetric.feed.consumer.store.FeedTracker
import com.qmetric.feed.consumer.store.SimpleDBFeedTracker
import com.theoryinpractise.halbuilder.api.ReadableRepresentation
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.mockito.Mockito
import spark.Spark

import static com.amazonaws.regions.Region.getRegion
import static java.lang.System.currentTimeMillis
import static java.util.concurrent.TimeUnit.MINUTES
import static java.util.concurrent.TimeUnit.SECONDS
import static junit.framework.Assert.fail
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.MatcherAssert.assertThat
import static org.mockito.Matchers.any
import static org.mockito.Mockito.times
import static org.mockito.Mockito.verify

class IntegrationTest
{
    private static final FEED_SIZE = 9
    private static final PAGE_SIZE = 3
    private static final FEED_SERVER_PORT = 15000
    private static final MAX_RETRY = 100
    private static final String SIMPLEDB_USER = 'hal-feed-consumer-test'
    private static final String SIMPLEDB_ACCESS_KEY = 'AKIAJXSKAGITLBPP6DVA'
    private static final String SIMPLEDB_SECRET_KEY = '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'
    private static final AmazonSimpleDBClient simpleDBClient = simpleDBClient()
    private static final domainName = "${SIMPLEDB_USER}-${currentTimeMillis()}".toString()
    private static FeedTracker tracker = new SimpleDBFeedTracker(simpleDBClient, domainName)

    @BeforeClass public static void startupServer()
    {
        Spark.setPort(FEED_SERVER_PORT)
        Spark.get(new MockFeedHandler("/feed", FEED_SIZE, PAGE_SIZE))
        Spark.get(new MockEntryHandler())
    }

    @BeforeClass public static void waitForDomainCreation()
    {
        boolean domainCreated = false
        int count = 0
        while (!domainCreated && count < MAX_RETRY)
        {
            try
            {
                simpleDBClient.domainMetadata(new DomainMetadataRequest(domainName))
                domainCreated = true
                println "Using domain: ${domainName}"
            }
            catch (Exception e)
            {
                count++
                println "${count} waiting for domain ${domainName} to be available"
                SECONDS.sleep(10)
            }
        }
        if (!domainCreated)
        {
            fail("Exceeded domain creation timeout")
        }
    }

    @AfterClass public static void deleteSimpleDBDomain()
    {
        simpleDBClient.deleteDomain(new DeleteDomainRequest(domainName))
    }

    @Test(timeout = 60000L) public void 'action is invoked as many times as the feed size'()
    {

        def action = Mockito.mock(ConsumeAction)

        def consumer = new FeedConsumerConfiguration()
                .consumeEachEntryWith(action)
                .withFeedTracker(tracker)
                .pollForNewEntriesEvery(30, SECONDS)
                .fromUrl("http://localhost:${FEED_SERVER_PORT}/feed").build()
        consumer.start()
        while (consumer.getInvocationsCount() < 1)
        {
            SECONDS.sleep(5)
        }
        consumer.stop()
        verify(action, times(FEED_SIZE)).consume(any(ReadableRepresentation))
        def result = simpleDBClient.select(new SelectRequest("select * from `${domainName}`", true))
        assertThat(result.items.size(), equalTo(FEED_SIZE))
    }

    private static AmazonSimpleDBClient simpleDBClient()
    {
        new AmazonSimpleDBClient(new BasicAWSCredentials(SIMPLEDB_ACCESS_KEY, SIMPLEDB_SECRET_KEY)).with {
            region = getRegion(Regions.EU_WEST_1)
            it
        }
    }
}
