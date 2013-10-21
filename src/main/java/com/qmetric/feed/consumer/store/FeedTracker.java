package com.qmetric.feed.consumer.store;

import com.theoryinpractise.halbuilder.api.Link;

public interface FeedTracker
{
    void checkConnectivity() throws ConnectivityException;

    void markAsConsuming(final Link link) throws AlreadyConsumingException;

    void revertConsuming(final Link link);

    void markAsConsumed(Link link);

    boolean isTracked(Link link);

    void track(Link link);

    Iterable<Link> getItemsToBeConsumed();
}
