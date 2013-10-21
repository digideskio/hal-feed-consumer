package com.qmetric.feed.consumer.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Timer;
import com.qmetric.feed.consumer.FeedConsumer_;
import com.theoryinpractise.halbuilder.api.Link;

import java.util.List;

public class FeedConsumerWithMetrics implements FeedConsumer_
{
    private static final int MAX_SAMPLES = 100;

    private final Timer consumptionTimer;

    private final Meter consumptionErrors;

    private final Meter consumptionSuccess;

    private final Meter numberOfConsumedEntries;

    private final FeedConsumer_ next;

    public FeedConsumerWithMetrics(final MetricRegistry metricRegistry, final FeedConsumer_ next)
    {
        consumptionTimer = metricRegistry.register("feedPolling.timeTaken", new Timer(new SlidingWindowReservoir(MAX_SAMPLES)));
        consumptionErrors = metricRegistry.meter("feedPolling.errors");
        consumptionSuccess = metricRegistry.meter("feedPolling.success");
        numberOfConsumedEntries = metricRegistry.meter("feedPolling.consumedEntries");

        this.next = next;
    }

    @Override public List<Link> consume() throws Exception
    {
        final Timer.Context context = consumptionTimer.time();

        try
        {
            final List<Link> consumed = next.consume();

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
