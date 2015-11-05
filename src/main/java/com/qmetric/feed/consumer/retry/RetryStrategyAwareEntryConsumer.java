package com.qmetric.feed.consumer.retry;

import com.qmetric.feed.consumer.DateTimeSource;
import com.qmetric.feed.consumer.EntryConsumer;
import com.qmetric.feed.consumer.TrackedEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryStrategyAwareEntryConsumer implements EntryConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(RetryStrategyAwareEntryConsumer.class);

    private final EntryConsumer next;
    private final RetryStrategy retryStrategy;
    private final DateTimeSource dateTimeSource;

    public RetryStrategyAwareEntryConsumer(EntryConsumer next, RetryStrategy retryStrategy, DateTimeSource dateTimeSource) {
        this.next = next;
        this.retryStrategy = retryStrategy;
        this.dateTimeSource = dateTimeSource;
    }

    @Override
    public boolean consume(TrackedEntry trackedEntry) throws Exception {
        if (trackedEntry.canBeProcessed(retryStrategy, dateTimeSource.now())) {
            LOG.info("Consuming entry {}", trackedEntry);
            return next.consume(trackedEntry);
        } else {
            LOG.info("Due to retry strategy skipping entry {}", trackedEntry);
            return false;
        }
    }
}
