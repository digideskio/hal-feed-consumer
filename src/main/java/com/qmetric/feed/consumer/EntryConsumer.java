package com.qmetric.feed.consumer;

public interface EntryConsumer
{
    boolean consume(TrackedEntry trackedEntry) throws Exception;
}
