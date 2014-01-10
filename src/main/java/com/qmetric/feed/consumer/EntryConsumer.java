package com.qmetric.feed.consumer;

public interface EntryConsumer
{
    void consume(EntryId id) throws Exception;
}
