package com.qmetric.feed.consumer;

import com.google.common.base.Optional;
import com.qmetric.hal.reader.HalResource;

public interface ResourceResolver
{
    Optional<HalResource> resolve(EntryId id);
}
