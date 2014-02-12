package com.qmetric.feed.consumer;

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
}
