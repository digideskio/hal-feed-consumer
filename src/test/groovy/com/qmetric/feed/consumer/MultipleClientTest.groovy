package com.qmetric.feed.consumer

import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Optional
import com.qmetric.feed.consumer.store.SimpleDBFeedTracker
import com.qmetric.feed.consumer.utils.SimpleDBUtils
import com.qmetric.hal.reader.HalResource
import com.theoryinpractise.halbuilder.api.RepresentationFactory
import com.theoryinpractise.halbuilder.impl.representations.MutableRepresentation
import org.junit.After
import org.junit.Before
import org.junit.Test

import java.util.concurrent.Executors
import java.util.concurrent.Future

import static com.qmetric.feed.consumer.DomainNameFactory.userPrefixedDomainName
import static java.lang.System.getenv
import static java.lang.Thread.currentThread
import static java.util.Collections.emptyList
import static java.util.concurrent.TimeUnit.SECONDS
import static org.hamcrest.core.IsEqual.equalTo
import static org.junit.Assert.assertThat
import static org.mockito.Mockito.mock

class MultipleClientTest {
    private static final FEED_SIZE = 9
    private static final String MARKER = 'throw-exception'
    private static final AmazonSimpleDBClient client = new SimpleDBClientFactory(getenv('HAL_CONSUMER_IT_AWS_ACCESS_KEY'), getenv('HAL_CONSUMER_IT_AWS_SECRET_KEY')).simpleDBClient()
    private static final String DOMAIN_NAME = userPrefixedDomainName('hal-feed-consumer-test')
    private static final SimpleDBFeedTracker tracker = new SimpleDBFeedTracker(client, DOMAIN_NAME)
    private static final executor = Executors.newFixedThreadPool(2)
    private static final resolver = new ResourceResolver() {
        @Override HalResource resolve(final EntryId id)
        {
            def representation = new HalResource(new ObjectMapper(), new MutableRepresentation(mock(RepresentationFactory), "/${id.toString()}"))
            return representation
        }
    }

    @Before public void setupDomain()
    {
        createDomain()
        populateDomain()
    }

    @After public void deleteDomain()
    {
        new SimpleDBUtils(client).deleteDomain(DOMAIN_NAME)
    }

    @Test public void 'a consumer picks up entries _not on the last page_ which another consumer previously failed to process'()
    {
        def slowActionThread = runConsumerInOwnThreadWith(slowFaultyAction)

        println "Test waiting for ${slowFaultyAction} to pick up the first feed entry, before it fails and reverts it"
        SECONDS.sleep(3)
        assertThat("${slowFaultyAction} is still consuming the first entry", tracker.countConsuming(), equalTo(1))
        assertThat("${slowFaultyAction} hasn't finished consuming the first entry", tracker.countConsumed(), equalTo(0))

        def fastActionThread = runConsumerInOwnThreadWith(quickRunningAction)
        println "Test waiting both consumers to complete"
        waitToComplete(fastActionThread, slowActionThread)

        assertThat("${slowFaultyAction} reverted consuming the first entry", tracker.countConsuming(), equalTo(FEED_SIZE - 1))
        assertThat("all other entries have been consumed", tracker.countConsumed(), equalTo(FEED_SIZE - 1))

        println "Test running ${quickRunningAction} again to pick up entries that ${slowFaultyAction} failed to process"
        newConsumer(quickRunningAction).consume()

        assertThat("all entries have been consumed", tracker.countConsumed(), equalTo(FEED_SIZE))
    }

    private static Future<?> runConsumerInOwnThreadWith(ConsumeAction runnable)
    {
        executor.submit(newRunnableConsumerWith(runnable))
    }

    private static void waitToComplete(Future... futures)
    {
        while (futures.any { f -> !f.done })
        {
            println "Spock is waiting for consumers to terminate"
            SECONDS.sleep 2
        }
    }

    private static Runnable newRunnableConsumerWith(ConsumeAction action)
    {
        newRunnable(newConsumer(action))
    }

    private static FeedConsumer newConsumer(ConsumeAction action)
    {
        def entryConsumer = new EntryConsumerImpl(tracker, action, resolver, emptyList(), Optional.absent())
        new FeedConsumerImpl(entryConsumer, tracker, emptyList())
    }

    private static Runnable newRunnable(FeedConsumer consumer)
    {
        new Runnable() {
            @Override void run()
            {
                println "Running ${consumer} in thread ${currentThread().name}"
                consumer.consume()
            }
        }
    }

    private static void populateDomain()
    {
        (1..FEED_SIZE).each { int it ->
            def entry = EntryId.of(it == 1 ? MARKER : "${it}")
            tracker.track(entry)
        }
    }

    private static void createDomain()
    {
        new SimpleDBUtils(client).createDomainAndWait(DOMAIN_NAME)
    }


    private static slowFaultyAction = new ConsumeAction() {
        @Override Result consume(final FeedEntry input)
        {
            if (input.content.resourceLink.get().href.contains(MARKER))
            {
                println "hang-and-fail-action waiting 10 sec before failing on ${input.content.getResourceLink()}"
                SECONDS.sleep(10)
                throw new RuntimeException()
            }
            else
            {
                println "hang-and-fail-action consumed ${input.content.getResourceLink()}"
            }

            Result.successful()
        }
    }

    private static quickRunningAction = new ConsumeAction() {
        @Override Result consume(final FeedEntry input)
        {
            println "quick-action consumed ${input.content.getResourceLink()}"
            Result.successful()
        }
    }
}
