package com.qmetric.feed.consumer.multipleClientsTest

import com.google.common.base.Function
import com.qmetric.feed.consumer.*
import com.sun.jersey.api.client.Client
import com.sun.net.httpserver.HttpServer
import com.theoryinpractise.halbuilder.api.ReadableRepresentation
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import sun.net.httpserver.DefaultHttpServerProvider

import javax.annotation.Nullable
import java.util.concurrent.Executors
import java.util.concurrent.Future

import static java.lang.Thread.currentThread
import static java.util.Collections.emptyList
import static java.util.concurrent.TimeUnit.*
import static org.hamcrest.core.IsEqual.equalTo
import static org.junit.Assert.assertThat

@Ignore
class MultipleClientTest
{
    private static final FEED_SIZE = 9
    private static final PAGE_SIZE = 3
    private static final FEED_SERVER_PORT = 15000
    private static final store = new InMemoryFeedTracker()
    private static final feedServer = getMockFeedServer(FEED_SERVER_PORT)
    private static final executor = Executors.newFixedThreadPool(2)

    private static final endpointFactory = new FeedEndpointFactory(new Client(),
                                                                   new FeedEndpointFactory.ConnectioTimeout(MINUTES, 1))
    private static final resolver = new DefaultResourceResolver(endpointFactory)

    @BeforeClass public static void startFeedServer()
    {
        feedServer.start()
    }

    @AfterClass public static void stopFeedServer()
    {
        def waitSeconds = 10
        feedServer.stop(waitSeconds)
    }

    @Test public void 'a consumer picks up entries /not on the last page/ which another consumer previsouly failed to process'()
    {
        def slowFaultyAction = new DelayedAction("slow-faulty-action", THROW_EXCEPTION, 10, SECONDS)
        def quickRunningAction = new DelayedAction("quick-running-action", DO_NOTHING, 100, MILLISECONDS)

        def slowActionThread = runConsumerInOwnThreadWith(slowFaultyAction)

        println "Test waiting for ${slowFaultyAction} to pick up the first feed entry, before it fails and reverts it"
        SECONDS.sleep(4)
        assertThat("${slowFaultyAction} is still consuming the first entry", store.consumingCount, equalTo(1))
        assertThat("${slowFaultyAction} hasn't finished consuming the first entry", store.consumedCount, equalTo(0))

        def fastActionThread = runConsumerInOwnThreadWith(quickRunningAction)
        println "Test waiting both consumers to complete"
        waitToComplete(fastActionThread, slowActionThread)

        assertThat("${slowFaultyAction} reverted consuming the first entry", store.consumingCount, equalTo(FEED_SIZE - 1))
        assertThat("all other entries have been consumed", store.consumedCount, equalTo(FEED_SIZE - 1))

        println "Test running ${quickRunningAction} again to pick up entries that ${slowFaultyAction} failed to process"
        newConsumer(quickRunningAction).consume()

        assertThat("all entries have been consumed", store.consumedCount, equalTo(FEED_SIZE))
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
        def entryConsumer = new EntryConsumerImpl(store, action, resolver, emptyList())
        new FeedConsumerImpl(entryConsumer, store, emptyList())
    }

    private static Runnable newRunnable(consumer)
    {
        new Runnable() {
            @Override void run()
            {
                println "Running ${consumer} in thread ${currentThread().name}"
                consumer.consume()
            }
        }
    }

    private static HttpServer getMockFeedServer(int port)
    {
        new DefaultHttpServerProvider()
                .createHttpServer(new InetSocketAddress(port), 0)
                .createContext("/feed", new MockFeedHandler(FEED_SIZE, PAGE_SIZE))
                .getServer()
    }

    private static final Function THROW_EXCEPTION = new Function<ReadableRepresentation, Void>() {
        @Override Void apply(@Nullable final ReadableRepresentation input)
        {
            throw new RuntimeException()
        }
    }

    public static final Function DO_NOTHING = new Function<ReadableRepresentation, Void>() {

        @Override Void apply(@Nullable final ReadableRepresentation input)
        {
            return new Void()
        }
    }
}
