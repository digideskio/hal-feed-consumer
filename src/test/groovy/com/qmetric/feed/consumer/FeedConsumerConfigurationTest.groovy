package com.qmetric.feed.consumer

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.health.HealthCheck
import com.codahale.metrics.health.HealthCheckRegistry
import com.google.common.base.Optional
import com.qmetric.feed.consumer.retry.AlwaysRetryingRetryStrategy
import com.qmetric.feed.consumer.retry.FibonacciDelayingRetryStrategy
import com.qmetric.feed.consumer.retry.RetryStrategy
import com.qmetric.feed.consumer.store.FeedTracker
import com.qmetric.hal.reader.HalReader
import org.joda.time.DateTime
import spock.lang.Specification

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

import static java.util.concurrent.TimeUnit.MINUTES

@SuppressWarnings("GroovyAccessibility")
class FeedConsumerConfigurationTest extends Specification {

    final feedConsumerConfiguration = new FeedConsumerConfiguration("name")

    def "should accept feed url"()
    {
        when:
        feedConsumerConfiguration.fromUrl("http://host/feed")

        then:
        feedConsumerConfiguration.feedUrl == "http://host/feed"
    }

    def "should accept consume action"()
    {
        given:
        final consumeAction = Mock(ConsumeAction)

        when:
        feedConsumerConfiguration.consumeEachEntryWith(consumeAction)

        then:
        feedConsumerConfiguration.consumeAction == consumeAction
    }

    def "should accept polling interval"()
    {
        when:
        feedConsumerConfiguration.pollForNewEntriesEvery(1, MINUTES)

        then:
        feedConsumerConfiguration.pollingInterval == new Interval(1, MINUTES)
    }

    def "should accept feed tracker"()
    {
        given:
        final feedTracker = Mock(FeedTracker)

        when:
        feedConsumerConfiguration.withFeedTracker(feedTracker)

        then:
        feedConsumerConfiguration.feedTracker == feedTracker
    }

    def "should earliest published date limit"()
    {
        given:
        final limit = new DateTime(2013, 8, 1, 12, 0, 0)

        when:
        feedConsumerConfiguration.ignoreEntriesEarlierThan(limit)

        then:
        feedConsumerConfiguration.earliestEntryLimit.get().date == limit
    }

    def "should accept limit on max retries"()
    {
        given:
        final maxRetries = 10

        when:
        feedConsumerConfiguration.withLimitOnNumberOfRetriesPerEntry(maxRetries)

        then:
        feedConsumerConfiguration.maxRetries == Optional.of(10)
    }

    def "should accept listeners"()
    {
        given:
        final listener = Mock(EntryConsumerListener)

        when:
        feedConsumerConfiguration.withListeners(listener)

        then:
        feedConsumerConfiguration.entryConsumerListeners == [listener]
    }

    def "should accept overridden hal reader"()
    {
        given:
        final HalReader halReader = Mock(HalReader)

        when:
        feedConsumerConfiguration.withHalReader(halReader)

        then:
        feedConsumerConfiguration.halReader == halReader
    }

    def "should accept metric registry"()
    {
        given:
        final registry = Mock(MetricRegistry)

        when:
        feedConsumerConfiguration.withMetricRegistry(registry)

        then:
        feedConsumerConfiguration.metricRegistry == registry
    }

    def "should accept health check registry"()
    {
        given:
        final registry = Mock(HealthCheckRegistry)

        when:
        feedConsumerConfiguration.withHealthCheckRegistry(registry)

        then:
        feedConsumerConfiguration.healthCheckRegistry == registry
    }

    def "should allow configuration of polling activity health check"()
    {
        when:
        feedConsumerConfiguration.withPollingActivityHealthCheck(15, MINUTES)

        then:
        feedConsumerConfiguration.pollingActivityHealthCheck.isPresent()
    }

    def "should accept custom health check"()
    {
        given:
        final registry = Mock(HealthCheckRegistry)
        final healthCheck = Mock(HealthCheck)
        feedConsumerConfiguration.withHealthCheckRegistry(registry)

        when:
        feedConsumerConfiguration.addCustomHealthCheck("", healthCheck)

        then:
        registry.register("", healthCheck)
    }

    def "should accept resource resolver"()
    {
        given:
        final resourceResolver = Mock(ResourceResolver)

        when:
        feedConsumerConfiguration.withResourceResolver(resourceResolver)

        then:
        feedConsumerConfiguration.resourceResolver == Optional.of(resourceResolver)
    }

    def "should accept overridden scheduled executor service"()
    {
        given:
        final executorService = Mock(ScheduledExecutorService)

        when:
        feedConsumerConfiguration.withScheduledExecutorService(executorService)

        then:
        feedConsumerConfiguration.scheduledExecutorService == executorService;
    }

    def "should accept register shutdown hook"()
    {
        given:
        final registerShutdownHook = false

        when:
        feedConsumerConfiguration.registerShutdownHook(registerShutdownHook)

        then:
        feedConsumerConfiguration.registerShutdownHook == registerShutdownHook
    }

    def "should be able to change default retry strategy"() {
        given:
        RetryStrategy customStrategy = Mock(RetryStrategy)
        assert feedConsumerConfiguration.retryStrategy instanceof AlwaysRetryingRetryStrategy

        when:
        feedConsumerConfiguration.withCustomRetryStrategy(customStrategy)

        then:
        feedConsumerConfiguration.retryStrategy == customStrategy
    }


    def "should be able to switch to a predefined delayed retry strategy with base interval"() {
        when:
        feedConsumerConfiguration.withIncrementallyDelayingRetryStrategy(2, MINUTES)

        then:
        feedConsumerConfiguration.retryStrategy instanceof FibonacciDelayingRetryStrategy
        ((FibonacciDelayingRetryStrategy) feedConsumerConfiguration.retryStrategy).baseInterval == new Interval(2, MINUTES)
    }
}
