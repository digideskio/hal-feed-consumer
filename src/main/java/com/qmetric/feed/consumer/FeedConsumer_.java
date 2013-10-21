package com.qmetric.feed.consumer;

import com.theoryinpractise.halbuilder.api.Link;

import java.util.List;

public interface FeedConsumer_
{
    List<Link> consume() throws Exception;
}
