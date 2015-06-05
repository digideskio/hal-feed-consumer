package com.qmetric.feed.consumer;

import static org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals;
import static org.apache.commons.lang3.builder.HashCodeBuilder.reflectionHashCode;

/**
 * Success/ failure result of consuming a feed entry.
 */
public class Result
{
    public enum State
    {
        SUCCESSFUL,

        RETRY_UNSUCCESSFUL,

        ABORT_UNSUCCESSFUL
    }

    public final State state;

    private Result(final State state)
    {
        this.state = state;
    }

    /**
     * Successful result.
     *
     * @return Success result
     */
    public static Result successful()
    {
        return new Result(State.SUCCESSFUL);
    }

    /**
     * Failure result allowing future retries.
     *
     * @return Failure.
     */
    public static Result retryUnsuccessful()
    {
        return new Result(State.RETRY_UNSUCCESSFUL);
    }

    /**
     * Failure disallowing future retries.
     *
     * @return Failure, no retries
     */
    public static Result abortUnsuccessful()
    {
        return new Result(State.ABORT_UNSUCCESSFUL);
    }

    public boolean failure()
    {
        return state != State.SUCCESSFUL;
    }

    public boolean shouldRetry()
    {
        return state == State.RETRY_UNSUCCESSFUL;
    }

    @Override public boolean equals(final Object obj)
    {
        return reflectionEquals(this, obj);
    }

    @Override public int hashCode()
    {
        return reflectionHashCode(this);
    }

    @Override public String toString()
    {
        return state.name();
    }
}
