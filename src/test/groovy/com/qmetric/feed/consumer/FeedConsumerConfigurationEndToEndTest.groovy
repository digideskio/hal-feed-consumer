package com.qmetric.feed.consumer

import com.qmetric.feed.consumer.store.FeedTracker
import com.qmetric.feed.consumer.store.referenceimplementation.InMemoryFeedTracker
import com.qmetric.feed.consumer.utils.MockEntryHandler
import com.qmetric.feed.consumer.utils.MockFeedHandler
import com.qmetric.feed.consumer.utils.TestEnvironment
import com.qmetric.spark.authentication.AuthenticationDetails
import com.qmetric.spark.authentication.BasicAuthenticationFilter
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import spark.Spark

import static java.util.concurrent.TimeUnit.SECONDS
import static org.mockito.Matchers.any
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.times
import static org.mockito.Mockito.verify
import static org.mockito.Mockito.when

class FeedConsumerConfigurationEndToEndTest {

    private static final FEED_SERVER_PORT = 15000
    private static final FEED_SIZE = 9
    private static final PAGE_SIZE = 3

    private final FeedTracker tracker = new InMemoryFeedTracker(new DateTimeSource())

    private final ConsumeAction action = mock(ConsumeAction)

    private final FeedConsumerScheduler consumer
    private static final String FEED_USER = "user"
    private static final String FEED_PASSWORD = "pwd"

    public FeedConsumerConfigurationEndToEndTest()
    {
        TestEnvironment.verifyEnvironment()
        consumer = new FeedConsumerConfiguration("test-feed")
                .consumeEachEntryWith(action)
                .withFeedTracker(tracker)
                .pollForNewEntriesEvery(30, SECONDS)
                .fromUrl("http://localhost:${FEED_SERVER_PORT}/feed")
                .withAuthenticationCredentials(new FeedConsumerConfiguration.Credentials(FEED_USER, FEED_PASSWORD))
                .build()
    }

    @BeforeClass public static void startupServer()
    {
        Spark.setPort(FEED_SERVER_PORT)
        Spark.get(new MockFeedHandler("/feed", FEED_SIZE, PAGE_SIZE))
        Spark.before(new BasicAuthenticationFilter("/feed/*", new AuthenticationDetails(FEED_USER, FEED_PASSWORD)));
        Spark.get(new MockEntryHandler())
    }

    @Test(timeout = 60000L) public void 'all entries provided by the mock feed are stored'()
    {
        when(action.consume(any(FeedEntry.class))).thenReturn(Result.successful())
        Assert.assertFalse(tracker.isTracked(EntryId.of("1")))

        consumer.start()
        waitConsumerToRunOnce(consumer)
        consumer.stop()

        verify(action, times(FEED_SIZE)).consume(any(FeedEntry))
        Assert.assertTrue(tracker.isTracked(EntryId.of("1")))
    }

    private static void waitConsumerToRunOnce(FeedConsumerScheduler consumer)
    {
        while (consumer.getInvocationsCount() < 1)
        {
            SECONDS.sleep(5)
        }
    }
}
