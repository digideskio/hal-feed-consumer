package com.qmetric.feed.consumer

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.simpledb.AmazonSimpleDBClient

import static com.amazonaws.regions.Region.getRegion

class SimpleDBClientFactory
{

    private final String accessKey
    private final String secretKey

    public SimpleDBClientFactory(String accessKey, String secretKey)
    {
        this.accessKey = accessKey
        this.secretKey = secretKey
    }

    public AmazonSimpleDBClient simpleDBClient()
    {
        new AmazonSimpleDBClient(new BasicAWSCredentials(accessKey, secretKey)).with {
            region = getRegion(Regions.EU_WEST_1)
            it
        }
    }
}
