package com.qmetric.feed.consumer.store;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataResult;
import com.amazonaws.services.simpledb.model.GetAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.UpdateCondition;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.qmetric.feed.consumer.EntryId;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

import static com.google.common.collect.FluentIterable.from;
import static java.lang.String.format;
import static org.joda.time.DateTime.now;

public class SimpleDBFeedTracker implements FeedTracker
{
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss");

    private static final String CONDITIONAL_CHECK_FAILED_ERROR_CODE = "ConditionalCheckFailed";

    private static final String CONSUMED_DATE_ATTR = "consumed";

    private static final String CONSUMING_DATE_ATTR = "consuming";

    private static final String FAILURES_COUNT = "failures_count";

    private static final String MAX_FAILURES = "099";

    private static final String SELECT_ITEM_BY_NAME = "select * from `%s` where itemName() = '%s' limit 1";

    private static final String SELECT_ITEMS_TO_BE_CONSUMED = "select itemName() from `%s` where `" + CONSUMED_DATE_ATTR + "` is null " +
                                                              "and `" + CONSUMING_DATE_ATTR + "` is null " +
                                                              "and (`" + FAILURES_COUNT + "` is null or `" + FAILURES_COUNT + "` < '" + MAX_FAILURES + "') " +
                                                              "limit 50";

    private static final UpdateCondition IF_NOT_ALREADY_CONSUMING = new UpdateCondition().withName(CONSUMING_DATE_ATTR).withExists(false);

    private static final Function<Item, EntryId> ITEM_TO_ENTRY_ID = new Function<Item, EntryId>()
    {
        @Override public EntryId apply(final Item input)
        {
            return EntryId.of(input.getName());
        }
    };

    private static final Attribute ZERO_FAILURES = new Attribute(FAILURES_COUNT, "0");

    private static final Predicate<Attribute> IS_FAILURE_COUNT = new Predicate<Attribute>()
    {
        @Override public boolean apply(final Attribute input)
        {
            return FAILURES_COUNT.equals(input.getName());
        }
    };

    private static final String ZEROPADDED_INTEGER = "%03d";

    private final AmazonSimpleDB simpleDBClient;

    private final String domain;

    public SimpleDBFeedTracker(final AmazonSimpleDB simpleDBClient, final String domain)
    {
        this.simpleDBClient = simpleDBClient;
        this.domain = domain;

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

    @Override public void revertConsuming(final EntryId id)
    {
        run(deleteRequest(id, attribute(CONSUMING_DATE_ATTR)));
    }

    @Override public void fail(final EntryId id)
    {
        final GetAttributesResult response = run(getAttributeRequest(id, FAILURES_COUNT));
        final Optional<Attribute> failuresCount = from(response.getAttributes()).firstMatch(IS_FAILURE_COUNT);
        final int currentCount = Integer.valueOf(failuresCount.or(ZERO_FAILURES).getValue());
        final int nextCount = currentCount + 1;
        PutAttributesRequest request = putRequest(id, new ReplaceableAttribute(FAILURES_COUNT, format(ZEROPADDED_INTEGER, nextCount), true));
        run(request);
        revertConsuming(id);
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
        final ReplaceableAttribute seen_at = new ReplaceableAttribute().withName("seen_at").withValue(currentDate()).withReplace(false);
        run(putRequest(id, seen_at));
    }

    @Override public Iterable<EntryId> getItemsToBeConsumed()
    {
        return from(run(selectToBeConsumed())).transform(ITEM_TO_ENTRY_ID);
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
        String query = format(SELECT_ITEM_BY_NAME, domain, id);
        return new SelectRequest().withSelectExpression(query).withConsistentRead(true);
    }

    private SelectRequest selectToBeConsumed()
    {
        String query = format(SELECT_ITEMS_TO_BE_CONSUMED, domain);
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

    private GetAttributesRequest getAttributeRequest(final EntryId id, final String... attributes)
    {
        return new GetAttributesRequest(domain, id.toString()).withAttributeNames(attributes);
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

    private GetAttributesResult run(final GetAttributesRequest request)
    {
        return simpleDBClient.getAttributes(request);
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

    private static ReplaceableAttribute withCurrentDate(final String name)
    {
        return new ReplaceableAttribute().withName(name).withValue(currentDate()).withReplace(true);
    }

    private static String currentDate()
    {
        return DATE_FORMATTER.print(now());
    }
}
