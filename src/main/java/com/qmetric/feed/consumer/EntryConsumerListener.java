package com.qmetric.feed.consumer;

public interface EntryConsumerListener
{
    void consumed(final EntryId consumedEntry);
}
