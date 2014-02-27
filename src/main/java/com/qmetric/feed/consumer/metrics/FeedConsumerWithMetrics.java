package com.qmetric.feed.consumer.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Timer;
import com.qmetric.feed.consumer.FeedConsumer;
import com.qmetric.feed.consumer.TrackedEntry;

import java.util.List;

public class FeedConsumerWithMetrics implements FeedConsumer
{
    private static final int MAX_SAMPLES = 100;

    private final Timer consumptionTimer;

    private final Meter consumptionErrors;

    private final Meter consumptionSuccess;

    private final Meter numberOfConsumedEntries;

    private final FeedConsumer next;

    public FeedConsumerWithMetrics(final String baseMetricName, final MetricRegistry metricRegistry, final FeedConsumer next)
    {
        consumptionTimer = metricRegistry.register(String.format("%s: feedPolling.timeTaken", baseMetricName), new Timer(new SlidingWindowReservoir(MAX_SAMPLES)));
        consumptionErrors = metricRegistry.meter(String.format("%s: feedPolling.errors", baseMetricName));
        consumptionSuccess = metricRegistry.meter(String.format("%s: feedPolling.success", baseMetricName));
        numberOfConsumedEntries = metricRegistry.meter(String.format("%s: feedPolling.consumedEntries", baseMetricName));

        this.next = next;
    }

    @Override public List<TrackedEntry> consume() throws Exception
    {
        final Timer.Context context = consumptionTimer.time();

        try
        {
            final List<TrackedEntry> consumed = next.consume();

            numberOfConsumedEntries.mark(consumed.size());

            consumptionSuccess.mark();

            return consumed;
        }
        catch (Exception e)
        {
            consumptionErrors.mark();

            throw e;
        }
        finally
        {
            context.stop();
        }
    }
}
