package com.qmetric.feed.consumer.store.referenceimplementation

import com.qmetric.feed.consumer.store.FeedTracker
import com.qmetric.feed.consumer.store.FeedTrackerSpecification

class InMemoryFeedTrackerTest extends FeedTrackerSpecification {

    @Override
    protected FeedTracker feedTrackedImplementation() {
        new InMemoryFeedTracker(dateTimeSource)
    }
}
