package com.qmetric.feed.consumer;

import com.theoryinpractise.halbuilder.api.ReadableRepresentation;

public interface ResourceResolver
{
    ReadableRepresentation resolve(EntryId id);
}
