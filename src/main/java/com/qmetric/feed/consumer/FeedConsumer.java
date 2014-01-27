package com.qmetric.feed.consumer;

import java.util.List;

public interface FeedConsumer
{
    List<TrackedEntry> consume() throws Exception;
}
