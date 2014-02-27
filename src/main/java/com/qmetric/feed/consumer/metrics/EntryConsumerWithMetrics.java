package com.qmetric.feed.consumer.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Timer;
import com.qmetric.feed.consumer.EntryConsumer;
import com.qmetric.feed.consumer.TrackedEntry;

public class EntryConsumerWithMetrics implements EntryConsumer
{
    private static final int MAX_SAMPLES = 100;

    private final Timer timer;

    private final Meter errorMeter;

    private final Meter successMeter;

    private final EntryConsumer next;

    public EntryConsumerWithMetrics(final String baseMetricName, final MetricRegistry metricRegistry, final EntryConsumer next)
    {
        timer = metricRegistry.register(String.format("%s: entryConsumption.timeTaken", baseMetricName), new Timer(new SlidingWindowReservoir(MAX_SAMPLES)));
        errorMeter = metricRegistry.meter(String.format("%s: entryConsumption.errors", baseMetricName));
        successMeter = metricRegistry.meter(String.format("%s: entryConsumption.success", baseMetricName));

        this.next = next;
    }

    @Override public void consume(final TrackedEntry trackedEntry) throws Exception
    {
        final Timer.Context context = timer.time();

        try
        {
            next.consume(trackedEntry);

            successMeter.mark();
        }
        catch (Exception e)
        {
            errorMeter.mark();

            throw e;
        }
        finally
        {
            context.stop();
        }
    }
}
