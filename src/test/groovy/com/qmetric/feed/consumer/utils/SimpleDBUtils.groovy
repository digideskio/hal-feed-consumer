package com.qmetric.feed.consumer.utils

import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.amazonaws.services.simpledb.model.CreateDomainRequest
import com.amazonaws.services.simpledb.model.DeleteDomainRequest
import com.amazonaws.services.simpledb.model.DomainMetadataRequest

import static java.util.concurrent.TimeUnit.SECONDS
import static junit.framework.Assert.fail

public class SimpleDBUtils
{
    private final AmazonSimpleDBClient client
    private static final MAX_RETRY = 100

    public SimpleDBUtils(AmazonSimpleDBClient client)
    {
        this.client = client
    }

    public void createDomainAndWait(final String domainName)
    {
        client.createDomain(new CreateDomainRequest(domainName))


        boolean domainCreated = false
        int count = 0
        while (!domainCreated && count < MAX_RETRY)
        {
            try
            {
                client.domainMetadata(new DomainMetadataRequest(domainName))
                domainCreated = true
                println "Using domain: ${domainName}"
            }
            catch (Exception e)
            {
                count++
                println "${count} waiting for domain ${domainName} to be available"
                SECONDS.sleep(10)
            }
        }
        if (!domainCreated)
        {
            fail("Exceeded domain creation timeout")
        }
    }


    public void deleteDomain(final String domainName)
    {
        client.deleteDomain(new DeleteDomainRequest(domainName))
    }
}