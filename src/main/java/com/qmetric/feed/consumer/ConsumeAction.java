package com.qmetric.feed.consumer;

/**
 * Application-specific feed entry consumption.
 */
public interface ConsumeAction
{
    /**
     * Consume next entry from feed.
     * Any exception thrown from this method will have the same affect as returning Result.RetryUnsuccessful().
     *
     * @param feedEntry Feed entry to consume.
     *
     * @return Success or failure of consumption.
     */
    Result consume(FeedEntry feedEntry);
}
