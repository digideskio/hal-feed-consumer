package com.qmetric.feed.consumer;

import com.theoryinpractise.halbuilder.api.Link;

public interface EntryConsumer
{
    void consume(Link feedEntry) throws Exception;
}
