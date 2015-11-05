package com.qmetric.feed.consumer.retry;

import org.joda.time.DateTime;

public interface RetryStrategy {
    boolean canRetry(int retriesSoFar, DateTime seenAt, DateTime currentTime);
}
