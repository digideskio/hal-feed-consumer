package com.qmetric.feed.consumer.store;

public class EntryNotTrackedException extends RuntimeException {

    public EntryNotTrackedException(String msg) {
        super(msg);
    }
}
