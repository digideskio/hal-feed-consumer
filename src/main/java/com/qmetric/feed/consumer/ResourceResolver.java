package com.qmetric.feed.consumer;

import com.theoryinpractise.halbuilder.api.Link;
import com.theoryinpractise.halbuilder.api.ReadableRepresentation;

public interface ResourceResolver
{
    ReadableRepresentation resolve(Link link);
}
