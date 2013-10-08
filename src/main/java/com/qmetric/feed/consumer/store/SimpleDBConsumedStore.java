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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.theoryinpractise.halbuilder.api.ReadableRepresentation;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

import static com.google.common.collect.FluentIterable.from;
import static java.lang.String.format;
import static org.joda.time.DateTime.now;

public class SimpleDBConsumedStore implements ConsumedStore
{
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss");

    private static final String CONDITIONAL_CHECK_FAILED_ERROR_CODE = "ConditionalCheckFailed";

    private static final String ID_PROPERTY = "_id";

    private static final String CONSUMED_DATE_ATTR = "consumed";

    private static final String CONSUMING_DATE_ATTR = "consuming";

    private static final String SELECT_CONSUMED_ITEM = "select itemName() from `%s` where itemName() = '%s' and `%s` is not null limit 1";

    public static final UpdateCondition IF_NOT_ALREADY_CONSUMING = new UpdateCondition().withName(CONSUMING_DATE_ATTR).withExists(false);

    private final AmazonSimpleDB simpleDBClient;

    private final String domain;

    public SimpleDBConsumedStore(final AmazonSimpleDB simpleDBClient, final String domain)
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

    @Override public void markAsConsuming(final ReadableRepresentation feedEntry) throws AlreadyConsumingException
    {
        try
        {
            run(new PutAttributesRequest(domain, getId(feedEntry), asList(withCurrentDate(CONSUMING_DATE_ATTR)), IF_NOT_ALREADY_CONSUMING));
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

    @Override public void revertConsuming(final ReadableRepresentation feedEntry)
    {
        run(new DeleteAttributesRequest(domain, getId(feedEntry), asList(attribute(CONSUMING_DATE_ATTR))));
    }

    @Override public void markAsConsumed(final ReadableRepresentation feedEntry)
    {
        run(new PutAttributesRequest(domain, getId(feedEntry), asList(withCurrentDate(CONSUMED_DATE_ATTR))));
    }

    @Override public boolean notAlreadyConsumed(final ReadableRepresentation feedEntry)
    {
        return !getConsumedEntry(feedEntry).isPresent();
    }

    private Optional<Item> getConsumedEntry(final ReadableRepresentation feedEntry)
    {
        return from(run(selectUnconsumed(feedEntry))).first();
    }

    private SelectRequest selectUnconsumed(final ReadableRepresentation feedEntry)
    {
        String query = format(SELECT_CONSUMED_ITEM, domain, getId(feedEntry), CONSUMED_DATE_ATTR);
        return new SelectRequest().withSelectExpression(query).withConsistentRead(true);
    }

    private DomainMetadataResult getDomainMetadata()
    {
        return run(new DomainMetadataRequest(domain));
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

    private static <T> ImmutableList<T> asList(T... ts)
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

    private static String getId(final ReadableRepresentation feedEntry)
    {
        return (String) feedEntry.getValue(ID_PROPERTY);
    }
}
