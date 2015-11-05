package com.qmetric.feed.consumer.retry;

import com.qmetric.feed.consumer.Interval;
import org.joda.time.DateTime;
import java.util.List;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public class FibonacciDelayingRetryStrategy implements RetryStrategy {

    public final Interval baseInterval;

    private final static List<Integer> FIBONACCI_SEQUENCE = unmodifiableList(asList(0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233));

    public FibonacciDelayingRetryStrategy(Interval baseInterval) {
        this.baseInterval = baseInterval;
    }

    @Override
    public boolean canRetry(final int retriesSoFar, final DateTime timeOfLastOperation, final DateTime currentTime) {
        if (retriesSoFar <= 0) {
            return true;
        } else {
            DateTime timeAfterWhichNextRetryCanTakePlace = calculateTimeAfterWhichNextRetryCanTakePlace(retriesSoFar, timeOfLastOperation);
            return currentTime.isAfter(timeAfterWhichNextRetryCanTakePlace);
        }
    }

    private DateTime calculateTimeAfterWhichNextRetryCanTakePlace(int retriesSoFar, DateTime timeOfLastOperation) {
        int delayFactor = calculateDelayFactor(retriesSoFar);
        return timeOfLastOperation.plus(baseInterval.times(delayFactor).asMillis());
    }

    private static Integer calculateDelayFactor(int retriesSoFar) {
        return FIBONACCI_SEQUENCE.get(normalizeRetries(retriesSoFar));
    }

    private static int normalizeRetries(int retriesSoFar) {
        int lastIndexFromSequence = FIBONACCI_SEQUENCE.size() - 1;
        int result = (retriesSoFar >= 0) ? retriesSoFar : 0;
        result = (result <= lastIndexFromSequence) ? result : lastIndexFromSequence;

        return result;
    }
}
