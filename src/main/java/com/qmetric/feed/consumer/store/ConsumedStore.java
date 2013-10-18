package com.qmetric.feed.consumer.store;

import com.theoryinpractise.halbuilder.api.Link;

public interface ConsumedStore
{
    void checkConnectivity() throws ConnectivityException;

    void markAsConsuming(final Link feedEntry) throws AlreadyConsumingException;

    void revertConsuming(final Link feedEntry);

    void markAsConsumed(Link feedEntry);

    boolean notAlreadyConsumed(Link feedEntry);

    Iterable<Link> getItemsToBeConsumed();
}
