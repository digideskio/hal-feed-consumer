package com.qmetric.feed.consumer.multipleClientsTest

import com.google.common.base.Function
import com.qmetric.feed.consumer.ConsumeAction
import com.theoryinpractise.halbuilder.api.ReadableRepresentation

import java.util.concurrent.TimeUnit

import static java.lang.Thread.currentThread

class DelayedAction implements ConsumeAction
{
    private final TimeUnit timeUnit
    private final int delay
    private final String name
    private final Function<ReadableRepresentation, Void> action

    public DelayedAction(final String name, final Function<ReadableRepresentation, Void> action, final int delay, final TimeUnit timeUnit)
    {
        this.action = action
        this.delay = delay
        this.timeUnit = timeUnit
        this.name = name
    }

    void consume(ReadableRepresentation feedEntry)
    {
        println "${name} consuming entry ${feedEntry} in thread ${currentThread().name}"
        if (feedEntry.getValue("type") == "error")
        {
            println "${name} hanging for ${timeUnit} ${delay}"
            timeUnit.sleep delay
            action.apply(feedEntry)
        }
    }

    @Override String toString()
    {
        return "<DelayedAction ${name}>"
    }
}
