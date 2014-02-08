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

    public FeedConsumerWithMetrics(final MetricRegistry metricRegistry, final FeedConsumer next)
    {
        consumptionTimer = metricRegistry.register("feedPolling.timeTaken", new Timer(new SlidingWindowReservoir(MAX_SAMPLES)));
        consumptionErrors = metricRegistry.meter("feedPolling.errors");
        consumptionSuccess = metricRegistry.meter("feedPolling.success");
        numberOfConsumedEntries = metricRegistry.meter("feedPolling.consumedEntries");

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
