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
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.theoryinpractise.halbuilder.DefaultRepresentationFactory;
import com.theoryinpractise.halbuilder.api.Link;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;

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

    private static final String SELECT_CONSUMED_ITEM = "select itemName() from `%s` where itemName() = '%s' and `" + CONSUMED_DATE_ATTR + "` is not null limit 1";

    private static final String SELECT_ITEMS_TO_BE_CONSUMED = "select itemName() from `%s` where `" + CONSUMED_DATE_ATTR +
                                                              "` is null and `" + CONSUMING_DATE_ATTR +
                                                              "` is null";

    public static final UpdateCondition IF_NOT_ALREADY_CONSUMING = new UpdateCondition().withName(CONSUMING_DATE_ATTR).withExists(false);

    public static final Function<Item, Link> ITEM_TO_LINK = new Function<Item, Link>()
    {
        @Nullable @Override public Link apply(final Item input)
        {
            return new Link(new DefaultRepresentationFactory(), "self", input.getName());
        }
    };

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

    @Override public void markAsConsuming(final Link feedEntry) throws AlreadyConsumingException
    {
        try
        {
            run(new PutAttributesRequest(domain, itemName(feedEntry), asList(withCurrentDate(CONSUMING_DATE_ATTR)), IF_NOT_ALREADY_CONSUMING));
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

    @Override public void revertConsuming(final Link feedEntry)
    {
        run(new DeleteAttributesRequest(domain, itemName(feedEntry), asList(attribute(CONSUMING_DATE_ATTR))));
    }

    @Override public void markAsConsumed(final Link feedEntry)
    {
        run(putRequest(feedEntry, withCurrentDate(CONSUMED_DATE_ATTR)));
    }

    @Override public boolean isTracked(final Link feedEntry)
    {
        return getEntry(feedEntry).isPresent();
    }

    @Override public void track(final Link link)
    {
        final ReplaceableAttribute seen_at = new ReplaceableAttribute().withName("seen_at").withValue(currentDate()).withReplace(false);
        run(putRequest(link, seen_at));
    }

    @Override public Iterable<Link> getItemsToBeConsumed()
    {
        return FluentIterable.from(run(selectToBeConsumed())).transform(ITEM_TO_LINK);
    }

    public Integer countConsuming()
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

    private Optional<Item> getEntry(final Link feedEntry)
    {
        final List<Item> result = run(selectConsumed(feedEntry));
        return from(result).first();
    }

    private SelectRequest selectConsumed(final Link feedEntry)
    {
        String query = format(SELECT_CONSUMED_ITEM, domain, itemName(feedEntry));
        return new SelectRequest().withSelectExpression(query).withConsistentRead(true);
    }

    private SelectRequest selectToBeConsumed()
    {
        String query = format(SELECT_ITEMS_TO_BE_CONSUMED, domain);
        return new SelectRequest().withSelectExpression(query).withConsistentRead(true);
    }

    private SelectRequest count(final String notNullAttribute)
    {
        return new SelectRequest(format("select count(*) from `%s` where %s is not null", domain, notNullAttribute));
    }

    private DomainMetadataResult getDomainMetadata()
    {
        return run(new DomainMetadataRequest(domain));
    }

    private PutAttributesRequest putRequest(final Link link, final ReplaceableAttribute... seen_at)
    {
        return new PutAttributesRequest(domain, itemName(link), asList(seen_at));
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

    private static ReplaceableAttribute withCurrentDate(final String name)
    {
        return new ReplaceableAttribute().withName(name).withValue(currentDate()).withReplace(true);
    }

    private static String currentDate()
    {
        return DATE_FORMATTER.print(now());
    }

    private static String itemName(final Link feedEntry)
    {
        return feedEntry.getHref();
    }
}
