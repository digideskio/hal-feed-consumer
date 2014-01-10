package com.qmetric.feed.consumer;

import java.util.List;

public interface FeedPollingListener
{
    void consumed(final List<EntryId> consumedEntries);
}
