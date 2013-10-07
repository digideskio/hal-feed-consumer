package com.qmetric.feed.consumer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.google.common.base.Optional;
import com.qmetric.feed.consumer.metrics.ConsumedStoreConnectivityHealthCheck;
import com.qmetric.feed.consumer.metrics.EntryConsumerWithMetrics;
import com.qmetric.feed.consumer.metrics.FeedConnectivityHealthCheck;
import com.qmetric.feed.consumer.metrics.FeedConsumerWithMetrics;
import com.qmetric.feed.consumer.metrics.PollingActivityHealthCheck;
import com.qmetric.feed.consumer.store.ConsumedStore;
import com.sun.jersey.api.client.Client;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;

public class FeedConsumerConfiguration
{
    private final Collection<FeedPollingListener> feedPollingListeners = new ArrayList<FeedPollingListener>();

    private final Collection<EntryConsumerListener> entryConsumerListeners = new ArrayList<EntryConsumerListener>();

    private final Client feedClient = new Client();

    private final FeedEndpointFactory feedEndpointFactory = new FeedEndpointFactory(feedClient, new FeedEndpointFactory.ConnectioTimeout(MINUTES, 1));

    private HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();

    private MetricRegistry metricRegistry = new MetricRegistry();

    private String feedUrl;

    private Interval pollingInterval;

    private Optional<PollingActivityHealthCheck> pollingActivityHealthCheck;

    private ConsumeAction consumeAction;

    private ConsumedStore consumedStore;

    private Optional<EarliestEntryLimit> earliestEntryLimit = Optional.absent();

    public FeedConsumerConfiguration fromUrl(final String feedUrl)
    {
        this.feedUrl = feedUrl;

        return this;
    }

    public FeedConsumerConfiguration consumeEachEntryWith(final ConsumeAction consumeAction)
    {
        this.consumeAction = consumeAction;

        return this;
    }

    public FeedConsumerConfiguration pollForNewEntriesEvery(final long interval, final TimeUnit intervalUnit)
    {
        pollingInterval = new Interval(interval, intervalUnit);

        return this;
    }

    public FeedConsumerConfiguration withPollingActivityHealthCheck(final long minimumTimeBetweenActivity, final TimeUnit unit)
    {
        pollingActivityHealthCheck = Optional.of(new PollingActivityHealthCheck(new Interval(minimumTimeBetweenActivity, unit)));

        return this;
    }

    public FeedConsumerConfiguration withConsumedStore(final ConsumedStore consumedStore)
    {
        this.consumedStore = consumedStore;

        return this;
    }

    public FeedConsumerConfiguration ignoreEntriesEarlierThan(final DateTime dateTime)
    {
        earliestEntryLimit = Optional.of(new EarliestEntryLimit(dateTime));

        return this;
    }

    public FeedConsumerConfiguration withListeners(final EntryConsumerListener... listeners)
    {
        entryConsumerListeners.addAll(asList(listeners));

        return this;
    }

    public FeedConsumerConfiguration withMetricRegistry(final MetricRegistry metricRegistry)
    {
        this.metricRegistry = metricRegistry;

        return this;
    }

    public FeedConsumerConfiguration withHealthCheckRegistry(final HealthCheckRegistry healthCheckRegistry)
    {
        this.healthCheckRegistry = healthCheckRegistry;

        return this;
    }

    public FeedConsumerConfiguration addCustomHealthCheck(final String name, final HealthCheck healthCheck)
    {
        healthCheckRegistry.register(name, healthCheck);

        return this;
    }

    public HealthCheckRegistry getHealthCheckRegistry()
    {
        return healthCheckRegistry;
    }

    public MetricRegistry getMetricRegistry()
    {
        return metricRegistry;
    }

    public FeedConsumerConfiguration start()
    {
        validateConfiguration();

        configureHealthChecks();

        final EntryConsumer entryConsumer = new EntryConsumerWithMetrics(metricRegistry, new EntryConsumerImpl(consumedStore, consumeAction, entryConsumerListeners));

        final FeedConsumer consumer = new FeedConsumerWithMetrics(metricRegistry,
                                                                  new FeedConsumerImpl(feedUrl, feedEndpointFactory, entryConsumer, consumedStore, earliestEntryLimit,
                                                                                       feedPollingListeners));

        new FeedConsumerScheduler(consumer, pollingInterval).start();

        return this;
    }

    private void validateConfiguration()
    {
        checkNotNull(feedUrl, "Missing feed url");
        checkNotNull(pollingInterval, "Missing polling interval");
        checkNotNull(consumeAction, "Missing entry consumer action");
        checkNotNull(consumedStore, "Missing consumed store");
    }

    private void configureHealthChecks()
    {
        healthCheckRegistry.register("Feed connectivity", new FeedConnectivityHealthCheck(feedUrl, feedClient));

        healthCheckRegistry.register("Consumed store connectivity", new ConsumedStoreConnectivityHealthCheck(consumedStore));

        if (pollingActivityHealthCheck.isPresent())
        {
            healthCheckRegistry.register("Feed polling activity", pollingActivityHealthCheck.get());
            feedPollingListeners.add(pollingActivityHealthCheck.get());
            entryConsumerListeners.add(pollingActivityHealthCheck.get());
        }
    }
}
