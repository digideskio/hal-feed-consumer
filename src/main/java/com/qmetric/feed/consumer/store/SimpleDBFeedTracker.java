package com.qmetric.feed.consumer.store;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.UpdateCondition;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.qmetric.feed.consumer.DateTimeSource;
import com.qmetric.feed.consumer.EntryId;
import com.qmetric.feed.consumer.TrackedEntry;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

import static com.google.common.collect.FluentIterable.from;
import static java.lang.String.format;

public class SimpleDBFeedTracker implements FeedTracker
{
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss");

    private static final int MAX_RESULTS_ALLOWED_BY_SDB = 2500;

    private static final String CONDITIONAL_CHECK_FAILED_ERROR_CODE = "ConditionalCheckFailed";

    private static final String CONSUMED_DATE_ATTR = "consumed";

    private static final String CONSUMING_DATE_ATTR = "consuming";

    private static final String FAILURES_COUNT = "failures_count";

    private static final String SEEN_AT = "seen_at";

    private static final String ABORTED = "aborted";

    private static final String SELECT_ITEM_BY_NAME = "select * from `%s` where itemName() = '%s' limit 1";

    private static final String SELECT_ITEMS_TO_BE_CONSUMED = "select * from `%s` where `" + CONSUMED_DATE_ATTR + "` is null " +
                                                              "and `" + CONSUMING_DATE_ATTR + "` is null " +
                                                              "and `" + ABORTED + "` is null " +
                                                              "and `" + SEEN_AT + "` is not null " +
                                                              "order by `" + SEEN_AT + "` ASC " +
                                                              "limit " + MAX_RESULTS_ALLOWED_BY_SDB;

    private static final UpdateCondition IF_NOT_ALREADY_CONSUMING = new UpdateCondition().withName(CONSUMING_DATE_ATTR).withExists(false);

    private static final Function<Item, TrackedEntry> ITEM_TO_ENTRY = new Function<Item, TrackedEntry>()
    {
        @Override public TrackedEntry apply(final Item item)
        {
            final Optional<Attribute> failuresCountAttribute = from(item.getAttributes()).firstMatch(IS_FAILURE_COUNT);

            return new TrackedEntry(EntryId.of(item.getName()), failuresCountAttribute.isPresent() ? Integer.valueOf(failuresCountAttribute.get().getValue()) : 0);
        }
    };

    private static final Predicate<Attribute> IS_FAILURE_COUNT = new Predicate<Attribute>()
    {
        @Override public boolean apply(final Attribute input)
        {
            return FAILURES_COUNT.equals(input.getName());
        }
    };

    private final AmazonSimpleDB simpleDBClient;

    private final String domain;

    private final DateTimeSource dateTimeSource;

    public SimpleDBFeedTracker(final AmazonSimpleDB simpleDBClient, final String domain)
    {
        this(simpleDBClient, domain, new DateTimeSource());
    }

    SimpleDBFeedTracker(final AmazonSimpleDB simpleDBClient, final String domain, final DateTimeSource dateTimeSource)
    {
        this.simpleDBClient = simpleDBClient;
        this.domain = domain;
        this.dateTimeSource = dateTimeSource;

        simpleDBClient.createDomain(new CreateDomainRequest(domain));
    }

    @Override public void checkConnectivity() throws ConnectivityException
    {
        try
        {
            getDomainMetadata();
        }
        catch (final Exception e)
        {
            throw new ConnectivityException(e);
        }
    }

    @Override public void markAsConsuming(final EntryId id) throws AlreadyConsumingException
    {
        try
        {
            run(putRequest(id, IF_NOT_ALREADY_CONSUMING, withCurrentDate(CONSUMING_DATE_ATTR)));
        }
        catch (final AmazonServiceException e)
        {
            if (CONDITIONAL_CHECK_FAILED_ERROR_CODE.equalsIgnoreCase(e.getErrorCode()))
            {
                throw new AlreadyConsumingException(e);
            }
            else
            {
                throw e;
            }
        }
    }

    @Override public void fail(final TrackedEntry trackedEntry, final boolean scheduleRetry)
    {
        final PutAttributesRequest failureUpdate;

        if (scheduleRetry)
        {
            final String updatedFailuresCount = String.valueOf(trackedEntry.retries + 1);
            failureUpdate = putRequest(trackedEntry.id, new ReplaceableAttribute(FAILURES_COUNT, updatedFailuresCount, true), buildTrackingAttribute());

        }
        else
        {
            failureUpdate = putRequest(trackedEntry.id, new ReplaceableAttribute(ABORTED, currentDate(), true));
        }

        run(failureUpdate);

        revertConsuming(trackedEntry.id);
    }

    @Override public void markAsConsumed(final EntryId id)
    {
        run(putRequest(id, withCurrentDate(CONSUMED_DATE_ATTR)));
    }

    @Override public boolean isTracked(final EntryId feedEntry)
    {
        return getEntry(feedEntry).isPresent();
    }

    @Override public void track(final EntryId id)
    {
        final ReplaceableAttribute seen_at = buildTrackingAttribute();
        run(putRequest(id, seen_at));
    }

    private ReplaceableAttribute buildTrackingAttribute()
    {
        return new ReplaceableAttribute().withName(SEEN_AT).withValue(currentDate()).withReplace(true);
    }

    private void revertConsuming(final EntryId id)
    {
        run(deleteRequest(id, attribute(CONSUMING_DATE_ATTR)));
    }

    @Override public Iterable<TrackedEntry> getEntriesToBeConsumed()
    {
        return from(run(selectToBeConsumed())).transform(ITEM_TO_ENTRY);
    }

    public int countConsuming()
    {
        final Item item = run(count(CONSUMING_DATE_ATTR)).get(0);
        final String count = item.getAttributes().get(0).getValue();
        return Integer.valueOf(count);
    }

    public int countConsumed()
    {
        final Item item = run(count(CONSUMED_DATE_ATTR)).get(0);
        final String count = item.getAttributes().get(0).getValue();
        return Integer.valueOf(count);
    }

    private Optional<Item> getEntry(final EntryId id)
    {
        final List<Item> result = run(selectItem(id));
        return from(result).first();
    }

    private SelectRequest selectItem(final EntryId id)
    {
        final String query = format(SELECT_ITEM_BY_NAME, domain, id);
        return new SelectRequest().withSelectExpression(query).withConsistentRead(true);
    }

    private SelectRequest selectToBeConsumed()
    {
        final String query = format(SELECT_ITEMS_TO_BE_CONSUMED, domain);
        return new SelectRequest().withSelectExpression(query).withConsistentRead(true);
    }

    private SelectRequest count(final String notNullAttribute)
    {
        return new SelectRequest(format("select count(*) from `%s` where %s is not null", domain, notNullAttribute), true);
    }

    private DomainMetadataResult getDomainMetadata()
    {
        return run(new DomainMetadataRequest(domain));
    }

    private PutAttributesRequest putRequest(final EntryId id, final ReplaceableAttribute... attributes)
    {
        return new PutAttributesRequest(domain, id.toString(), asList(attributes));
    }

    private PutAttributesRequest putRequest(final EntryId id, final UpdateCondition updateCondition, final ReplaceableAttribute... attributes)
    {
        return new PutAttributesRequest(domain, id.toString(), asList(attributes), updateCondition);
    }

    private DeleteAttributesRequest deleteRequest(final EntryId id, final Attribute... attributes)
    {
        return new DeleteAttributesRequest(domain, id.toString(), asList(attributes));
    }

    private List<Item> run(final SelectRequest request)
    {
        return simpleDBClient.select(request).getItems();
    }

    private void run(final PutAttributesRequest request)
    {
        simpleDBClient.putAttributes(request);
    }

    private void run(final DeleteAttributesRequest request)
    {
        simpleDBClient.deleteAttributes(request);
    }

    private DomainMetadataResult run(final DomainMetadataRequest request)
    {
        return simpleDBClient.domainMetadata(request);
    }

    private Attribute attribute(final String name)
    {
        return new Attribute().withName(name);
    }

    private static <T> List<T> asList(T... ts)
    {
        return ImmutableList.<T>builder().add(ts).build();
    }

    private ReplaceableAttribute withCurrentDate(final String name)
    {
        return new ReplaceableAttribute().withName(name).withValue(currentDate()).withReplace(true);
    }

    private String currentDate()
    {
        return DATE_FORMATTER.print(dateTimeSource.now());
    }
}
