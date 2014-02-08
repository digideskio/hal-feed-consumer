package com.qmetric.feed.consumer;

public interface EntryConsumer
{
    void consume(TrackedEntry trackedEntry) throws Exception;
}
