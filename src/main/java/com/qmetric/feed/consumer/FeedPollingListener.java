package com.qmetric.feed.consumer;

import com.theoryinpractise.halbuilder.api.Link;

import java.util.List;

public interface FeedPollingListener
{
    void consumed(final List<Link> consumedEntries);
}
