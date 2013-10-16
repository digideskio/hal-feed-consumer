package com.qmetric.feed.consumer.multipleClientsTest

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import groovy.json.JsonOutput

import static groovy.json.JsonOutput.toJson
import static java.util.Collections.emptyMap
import static org.apache.commons.lang3.StringUtils.isBlank

class MockFeedHandler implements HttpHandler
{
    private final int feedSize
    private final int pageSize

    public MockFeedHandler(int feedSize, int pageSize)
    {
        this.pageSize = pageSize
        this.feedSize = feedSize
    }

    @Override void handle(final HttpExchange httpExchange) throws IOException
    {
        httpExchange.with {
            def params = parse(requestURI.query)
            def entries = generateEntries(pageIndex(params))
            sendResponseHeaders(200, 0)
            responseBody.withWriter { Writer w -> w.write(entries) }
            responseBody.close()
        }
    }

    private int pageIndex(Map params)
    {
        String pageIndex = params.get('upToEntryId') ?: lastEntryIndex as String
        Integer.valueOf(pageIndex)
    }

    private int getLastEntryIndex()
    {
        feedSize
    }

    private static Map parse(final String query)
    {
        if (!isBlank(query))
        {
            query.split('&').collectEntries { pair -> def (k, v) = pair.split('='); return [k, v] }
        }
        else
        {
            emptyMap()
        }
    }

    private generateEntries(int upToEntryId)
    {
        def fromEntryId = pageStart(upToEntryId)
        def entries = (fromEntryId..upToEntryId).reverse().collect { eId ->
            [//
                    _id: "${eId}",
                    _published: "24/05/2013 00:0${eId}:00",
                    _links: [self: [href: "/feed/${eId}"]],
                    type: (eId == 1 ? "error" : "ok") //
            ]
        }

        def page = [_links: [self: [href: "http://localhost:15000/feed?upToEntryId=${upToEntryId}"]], _embedded: [entries: entries]]
        if (fromEntryId > 1)
        {
            page._links.next = [href: "http://localhost:15000/feed?upToEntryId=${fromEntryId - 1}"]
        }
        return toJson(page)
    }

    private int pageStart(int lastEntryIndex)
    {
        lastEntryIndex > pageSize ? lastEntryIndex - pageSize + 1 : 1
    }
}