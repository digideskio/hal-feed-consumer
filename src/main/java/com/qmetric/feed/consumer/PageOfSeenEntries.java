package com.qmetric.feed.consumer;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.collect.FluentIterable.from;

public class PageOfSeenEntries
{
    private final List<SeenEntry> all;

    public PageOfSeenEntries(final List<SeenEntry> all)
    {
        this.all = all;
    }

    public List<SeenEntry> all()
    {
        return all;
    }

    public ImmutableList<SeenEntry> untrackedEntries()
    {
        return from(all).filter(new Predicate<SeenEntry>()
        {
            @Override public boolean apply(final SeenEntry input)
            {
                return input.notAlreadyTracked();
            }
        }).toList();
    }

    public PageOfSeenEntries reverse()
    {
        return new PageOfSeenEntries(from(all).toList().reverse());
    }
}
