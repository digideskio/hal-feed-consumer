package com.qmetric.feed.consumer.store.referenceimplementation;

import com.qmetric.feed.consumer.DateTimeSource;
import com.qmetric.feed.consumer.EntryId;
import com.qmetric.feed.consumer.SeenEntry;
import com.qmetric.feed.consumer.TrackedEntry;
import com.qmetric.feed.consumer.store.AlreadyConsumingException;
import com.qmetric.feed.consumer.store.ConnectivityException;
import com.qmetric.feed.consumer.store.EntryNotTrackedException;
import com.qmetric.feed.consumer.store.FeedTracker;
import org.joda.time.DateTime;

import java.util.*;

public class InMemoryFeedTracker implements FeedTracker {

    private final DateTimeSource dateTimeSource;
    private final Map<EntryId, Item> items = new HashMap<EntryId, Item>();
    private static final Comparator<Item> ITEM_BY_UPDATE_TIME_ASC = new ItemComparatorByTimeAsc();


    public InMemoryFeedTracker(DateTimeSource dateTimeSource) {
        this.dateTimeSource = dateTimeSource;
    }

    @Override
    public void checkConnectivity() throws ConnectivityException {}

    @Override
    public boolean isTracked(EntryId id) {
        return items.containsKey(id);
    }

    @Override
    public void track(SeenEntry entry) {
        if(!isTracked(entry.id)) {
            Item item = new Item(toTrackedEntry(entry), Status.SEEN, dateTimeSource.now());
            items.put(item.getEntryId(), item);
        }
    }

    @Override
    public Iterable<TrackedEntry> getEntriesToBeConsumed() {
        List<Item> toBeConsumed = new ArrayList<Item>();
        for (Item item : items.values()) { if (item.status == Status.SEEN) { toBeConsumed.add(item); } }
        Collections.sort(toBeConsumed, ITEM_BY_UPDATE_TIME_ASC);

        List < TrackedEntry > result = new ArrayList<TrackedEntry>(toBeConsumed.size());
        for (Item item : toBeConsumed) { result.add(item.trackedEntry); }

        return result;
    }

    @Override
    public void markAsConsuming(EntryId id) throws AlreadyConsumingException {
        if (!isTracked(id)) {
            throw new EntryNotTrackedException("Entry of id="+id+" is not tracked");
        }
        if (items.get(id).status == Status.CONSUMING) {
            throw new AlreadyConsumingException();
        } else {
            items.put(id, items.get(id).toConsuming(dateTimeSource.now()));
        }
    }

    @Override
    public void markAsConsumed(EntryId id) {
        items.put(id, items.get(id).toConsumed(dateTimeSource.now()));
    }

    @Override
    public void fail(TrackedEntry trackedEntry, boolean scheduleRetry) {
        Status nextStatus = scheduleRetry ? Status.SEEN : Status.ABORTED;
        TrackedEntry updatedEntry = new TrackedEntry(trackedEntry.id, trackedEntry.created, dateTimeSource.now(), trackedEntry.retries + 1);
        items.put(trackedEntry.id, new Item(updatedEntry, nextStatus, dateTimeSource.now()));
    }

    private TrackedEntry toTrackedEntry(SeenEntry entry) {
        return new TrackedEntry(entry.id, entry.dateTime, dateTimeSource.now(), 0);
    }

    private enum Status {
        SEEN,
        CONSUMING,
        CONSUMED,
        ABORTED
    }

    private static class ItemComparatorByTimeAsc implements Comparator<Item> {

        @Override
        public int compare(Item a, Item b) {
            return a.updated.isAfter(b.updated) ? 1 : a.updated.isEqual(b.updated) ? 0 : -1;
        }
    }

    private static class Item {
        public TrackedEntry trackedEntry;
        public Status status;
        public DateTime updated;


        Item(TrackedEntry trackedEntry, Status status, DateTime updated) {
            this.trackedEntry = trackedEntry;
            this.status = status;
            this.updated = updated;
        }

        public EntryId getEntryId() {
            return trackedEntry.id;
        }

        public Item toConsuming(DateTime timeOfUpdate) {
            return new Item(trackedEntry, Status.CONSUMING, timeOfUpdate);
        }

        public Item toConsumed(DateTime timeOfUpdate) {
            return new Item(trackedEntry, Status.CONSUMED, timeOfUpdate);
        }

    }
}
