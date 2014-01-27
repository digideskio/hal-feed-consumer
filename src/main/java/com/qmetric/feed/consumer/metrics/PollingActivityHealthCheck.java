package com.qmetric.feed.consumer.metrics;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Optional;
import com.qmetric.feed.consumer.DateTimeSource;
import com.qmetric.feed.consumer.EntryConsumerListener;
import com.qmetric.feed.consumer.EntryId;
import com.qmetric.feed.consumer.FeedPollingListener;
import com.qmetric.feed.consumer.Interval;
import com.qmetric.feed.consumer.TrackedEntry;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

import static com.codahale.metrics.health.HealthCheck.Result.healthy;
import static com.codahale.metrics.health.HealthCheck.Result.unhealthy;

public class PollingActivityHealthCheck extends HealthCheck implements FeedPollingListener, EntryConsumerListener
{
    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormat.fullDateTime();

    private final DateTimeSource dateTimeSource;

    private final Duration tolerableDelay;

    private Optional<DateTime> lastConsumed = Optional.absent();

    public PollingActivityHealthCheck(final Interval interval)
    {
        this(interval, new DateTimeSource());
    }

    PollingActivityHealthCheck(final Interval interval, DateTimeSource dateTimeSource)
    {
        this.tolerableDelay = new Duration(interval.unit.toMillis(interval.time));
        this.dateTimeSource = dateTimeSource;
    }

    @Override protected synchronized HealthCheck.Result check() throws Exception
    {
        return !lastConsumed.isPresent() || durationSinceLastConsumedIsLongerThanTolerableDelay() ? unhealthyResult() : healthyResult();
    }

    @Override public void consumed(final List<TrackedEntry> consumedEntries)
    {
        refreshLastConsumedDate();
    }

    @Override public void consumed(final EntryId consumedEntry)
    {
        refreshLastConsumedDate();
    }

    private synchronized void refreshLastConsumedDate()
    {
        this.lastConsumed = Optional.of(dateTimeSource.now());
    }

    private boolean durationSinceLastConsumedIsLongerThanTolerableDelay()
    {
        return new Duration(lastConsumed.get(), dateTimeSource.now()).isLongerThan(tolerableDelay);
    }

    private Result unhealthyResult()
    {
        if (lastConsumed.isPresent())
        {
            return unhealthy(String.format("No activity since %s ", lastConsumed.get().toString(DATE_TIME_FORMAT)));
        }
        else
        {
            return unhealthy("No activity");
        }
    }

    private Result healthyResult()
    {
        return healthy(String.format("Active at %s ", lastConsumed.get().toString(DATE_TIME_FORMAT)));
    }
}
