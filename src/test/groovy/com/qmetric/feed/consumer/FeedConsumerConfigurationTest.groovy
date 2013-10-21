package com.qmetric.feed.consumer

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.health.HealthCheck
import com.codahale.metrics.health.HealthCheckRegistry
import com.qmetric.feed.consumer.store.FeedTracker
import org.joda.time.DateTime
import spock.lang.Specification

import java.util.concurrent.TimeUnit

@SuppressWarnings("GroovyAccessibility")
class FeedConsumerConfigurationTest extends Specification
{

    final feedConsumerConfiguration = new FeedConsumerConfiguration()

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
        feedConsumerConfiguration.pollForNewEntriesEvery(1, TimeUnit.MINUTES)

        then:
        feedConsumerConfiguration.pollingInterval == new Interval(1, TimeUnit.MINUTES)
    }

    def "should accept consumed entry store"()
    {
        given:
        final consumedStore = Mock(FeedTracker)

        when:
        feedConsumerConfiguration.withConsumedStore(consumedStore)

        then:
        feedConsumerConfiguration.feedTracker == consumedStore
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

    def "should accept listeners"()
    {
        given:
        final listener = Mock(EntryConsumerListener_)

        when:
        feedConsumerConfiguration.withListeners(listener)

        then:
        feedConsumerConfiguration.entryConsumerListeners == [listener]
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
        feedConsumerConfiguration.withPollingActivityHealthCheck(15, TimeUnit.MINUTES)

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
        feedConsumerConfiguration.resourceResolver == resourceResolver
    }
}
