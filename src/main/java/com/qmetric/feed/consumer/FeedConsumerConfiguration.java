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
import com.qmetric.feed.consumer.store.FeedTracker;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.theoryinpractise.halbuilder.DefaultRepresentationFactory;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;

public class FeedConsumerConfiguration
{
    private final Collection<FeedPollingListener> feedPollingListeners = new ArrayList<FeedPollingListener>();

    private final Collection<EntryConsumerListener> entryConsumerListeners = new ArrayList<EntryConsumerListener>();

    private final Client feedClient = new Client();

    private final FeedEndpointFactory feedEndpointFactory = new FeedEndpointFactory(feedClient, new FeedEndpointFactory.ConnectionTimeout(MINUTES, 1));

    private HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();

    private MetricRegistry metricRegistry = new MetricRegistry();

    private String feedUrl;

    private Interval pollingInterval;

    private Optional<PollingActivityHealthCheck> pollingActivityHealthCheck = Optional.absent();

    private ConsumeAction consumeAction;

    private FeedTracker feedTracker;

    private Optional<EarliestEntryLimit> earliestEntryLimit = Optional.absent();

    private Optional<ResourceResolver> resourceResolver = Optional.absent();

    private Optional<Integer> maxRetries = Optional.absent();

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

    public FeedConsumerConfiguration withResourceResolver(final ResourceResolver resourceResolver)
    {
        this.resourceResolver = Optional.of(resourceResolver);

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

    public FeedConsumerConfiguration withFeedTracker(final FeedTracker feedTracker)
    {
        this.feedTracker = feedTracker;
        return this;
    }

    public FeedConsumerConfiguration ignoreEntriesEarlierThan(final DateTime dateTime)
    {
        earliestEntryLimit = Optional.of(new EarliestEntryLimit(dateTime));

        return this;
    }

    public FeedConsumerConfiguration withLimitOnNumberOfRetriesPerEntry(final Integer maxRetries)
    {
        checkState(maxRetries > 0, "Max retries must be more than 0");
        this.maxRetries = Optional.of(maxRetries);

        return this;
    }

    public FeedConsumerConfiguration withAuthenticationCredentials(final Credentials credentials)
    {
        feedClient.addFilter(new HTTPBasicAuthFilter(credentials.username, credentials.password));

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

    public FeedConsumerScheduler build()
    {
        validateConfiguration();

        configureHealthChecks();

        return buildConsumerScheduler();
    }

    private FeedConsumerScheduler buildConsumerScheduler()
    {
        return new FeedConsumerScheduler(feedConsumer(), feedEntriesFinder(), pollingInterval);
    }

    private AvailableFeedEntriesFinder feedEntriesFinder()
    {
        return new AvailableFeedEntriesFinder(feedEndpointFactory.create(feedUrl), feedEndpointFactory, feedTracker, earliestEntryLimit);
    }

    private FeedConsumer feedConsumer()
    {
        final FeedConsumer consumer = new FeedConsumerImpl(entryConsumer(), feedTracker, feedPollingListeners);
        return new FeedConsumerWithMetrics(metricRegistry, consumer);
    }

    private EntryConsumerWithMetrics entryConsumer()
    {
        return new EntryConsumerWithMetrics(metricRegistry, new EntryConsumerImpl(feedTracker, consumeAction, resourceResolver(), entryConsumerListeners, maxRetries));
    }

    private ResourceResolver resourceResolver()
    {
        return resourceResolver.or(new DefaultResourceResolver(feedUrl, feedEndpointFactory, new DefaultRepresentationFactory()));
    }

    private void validateConfiguration()
    {
        checkNotNull(feedUrl, "Missing feed url");
        checkNotNull(pollingInterval, "Missing polling interval");
        checkNotNull(consumeAction, "Missing entry consumer action");
        checkNotNull(feedTracker, "Missing consumed store");
        checkNotNull(resourceResolver(), "Missing resrouce resolver");
    }

    private void configureHealthChecks()
    {
        healthCheckRegistry.register("Feed connectivity", new FeedConnectivityHealthCheck(feedUrl, feedClient));

        healthCheckRegistry.register("Consumed store connectivity", new ConsumedStoreConnectivityHealthCheck(feedTracker));

        if (pollingActivityHealthCheck.isPresent())
        {
            healthCheckRegistry.register("Feed polling activity", pollingActivityHealthCheck.get());
            feedPollingListeners.add(pollingActivityHealthCheck.get());
            entryConsumerListeners.add(pollingActivityHealthCheck.get());
        }
    }

    public static class Credentials
    {
        public final String username;

        public final byte[] password;

        public Credentials(final String username, final byte[] password)
        {
            this.username = username;
            this.password = password;
        }
    }
}
