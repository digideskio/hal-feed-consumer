package com.qmetric.feed.consumer;

import com.qmetric.hal.reader.HalResource;

public interface ResourceResolver
{
    HalResource resolve(EntryId id);
}
