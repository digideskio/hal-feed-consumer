package com.qmetric.feed.consumer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.qmetric.feed.consumer.metrics.EntryConsumerWithMetrics;
import com.qmetric.feed.consumer.metrics.FeedConnectivityHealthCheck;
import com.qmetric.feed.consumer.metrics.FeedConsumerWithMetrics;
import com.qmetric.feed.consumer.metrics.FeedTrackerConnectivityHealthCheck;
import com.qmetric.feed.consumer.metrics.PollingActivityHealthCheck;
import com.qmetric.feed.consumer.store.FeedTracker;
import com.qmetric.hal.reader.HalReader;
import org.apache.http.client.HttpClient;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class FeedConsumerConfiguration
{
    private final Collection<FeedPollingListener> feedPollingListeners = new ArrayList<FeedPollingListener>();

    private final Collection<EntryConsumerListener> entryConsumerListeners = new ArrayList<EntryConsumerListener>();

    private HttpClient feedClient;

    private FeedEndpointFactory feedEndpointFactory;

    private final String name;

    private HalReader halReader = new HalReader(new ObjectMapper());

    private HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();

    private MetricRegistry metricRegistry = new MetricRegistry();

    private String feedUrl;

    private Interval pollingInterval;

    private final Interval missingEntriesTimeout = new Interval(15, MINUTES);

    private Optional<PollingActivityHealthCheck> pollingActivityHealthCheck = Optional.absent();

    private ConsumeAction consumeAction;

    private FeedTracker feedTracker;

    private Optional<EarliestEntryLimit> earliestEntryLimit = Optional.absent();

    private Optional<ResourceResolver> resourceResolver = Optional.absent();

    private Optional<Integer> maxRetries = Optional.absent();

    private boolean registerShutdownHook = true;

    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public FeedConsumerConfiguration(final String name)
    {
        this.name = name;
        updateFeedClient(defaultFeedClient());
    }

    private HttpClient defaultFeedClient()
    {
        return ClientBuilder.newHttpClient(60 * 1000);
    }

    private HttpClient defaultFeedClientWithBasicAuth(final Credentials credentials)
    {
        return ClientBuilder.newHttpClient(60 * 1000, credentials);
    }

    private void updateFeedClient(HttpClient feedClient)
    {
        this.feedClient = feedClient;
        this.feedEndpointFactory = new FeedEndpointFactory(feedClient);
    }

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
        updateFeedClient(defaultFeedClientWithBasicAuth(credentials));
        return this;
    }

    public FeedConsumerConfiguration withHalReader(final HalReader halReader)
    {
        this.halReader = halReader;

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

    public FeedConsumerConfiguration registerShutdownHook(boolean registerShutdownHook)
    {
        this.registerShutdownHook = registerShutdownHook;

        return this;
    }

    public FeedConsumerConfiguration withScheduledExecutorService(ScheduledExecutorService scheduledExecutorService)
    {
        this.scheduledExecutorService = scheduledExecutorService;

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
        return new FeedConsumerScheduler(feedConsumer(), feedEntriesTracker(), pollingInterval, scheduledExecutorService, registerShutdownHook, feedUrl);
    }

    private AvailableFeedEntriesTracker feedEntriesTracker()
    {
        return new AvailableFeedEntriesTracker(feedEndpointFactory.create(feedUrl), feedEndpointFactory, feedTracker, halReader, new NonContiguousEntryIdTracker(feedTracker), new PageOfSeenEntriesFactory(feedTracker, earliestEntryLimit));
    }

    private FeedConsumer feedConsumer()
    {
        final FeedConsumer consumer = new FeedConsumerImpl(entryConsumer(), feedTracker, feedPollingListeners);
        return new FeedConsumerWithMetrics(name, metricRegistry, consumer);
    }

    private EntryConsumerWithMetrics entryConsumer()
    {
        return new EntryConsumerWithMetrics(name, metricRegistry, new EntryConsumerImpl(feedTracker, consumeAction, resourceResolver(), entryConsumerListeners, maxRetries, missingEntriesTimeout, new DateTimeSource()));
    }

    private ResourceResolver resourceResolver()
    {
        return resourceResolver.or(new DefaultResourceResolver(feedUrl, feedEndpointFactory, halReader));
    }

    private void validateConfiguration()
    {
        checkArgument(isNotBlank(name), "Missing feed consumer name");
        checkNotNull(feedUrl, "Missing feed url");
        checkNotNull(pollingInterval, "Missing polling interval");
        checkNotNull(consumeAction, "Missing entry consumer action");
        checkNotNull(feedTracker, "Missing feed tracker");
        checkNotNull(resourceResolver(), "Missing resource resolver");
    }

    private void configureHealthChecks()
    {
        healthCheckRegistry.register(String.format("%s: Feed connectivity", name), new FeedConnectivityHealthCheck(feedUrl, feedClient));

        healthCheckRegistry.register(String.format("%s: Feed tracker store connectivity", name), new FeedTrackerConnectivityHealthCheck(feedTracker));

        if (pollingActivityHealthCheck.isPresent())
        {
            healthCheckRegistry.register(String.format("%s: Feed polling activity", name), pollingActivityHealthCheck.get());
            feedPollingListeners.add(pollingActivityHealthCheck.get());
            entryConsumerListeners.add(pollingActivityHealthCheck.get());
        }
    }

    public static class Credentials
    {
        public final String username;

        public final String password;

        public Credentials(final String username, final String password)
        {
            this.username = username;
            this.password = password;
        }
    }
}
