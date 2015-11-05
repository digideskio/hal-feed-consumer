package com.qmetric.feed.consumer.retry;

import org.joda.time.DateTime;

public class AlwaysRetryingRetryStrategy implements RetryStrategy {

    @Override
    public boolean canRetry(int retriesSoFar, DateTime seenAt, DateTime currentTime) {
        return true;
    }
}
