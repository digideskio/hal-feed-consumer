package com.qmetric.feed.consumer;

import java.util.List;

public interface FeedConsumer
{
    List<EntryId> consume() throws Exception;
}
