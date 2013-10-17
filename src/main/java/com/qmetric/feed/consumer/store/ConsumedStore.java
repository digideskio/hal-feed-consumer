package com.qmetric.feed.consumer.store;

public interface ConsumedStore<T>
{
    void checkConnectivity() throws ConnectivityException;

    void markAsConsuming(final T feedEntry) throws AlreadyConsumingException;

    void revertConsuming(final T feedEntry);

    void markAsConsumed(T feedEntry);

    boolean notAlreadyConsumed(T feedEntry);

    Iterable<T> getItemsToBeConsumed();
}
