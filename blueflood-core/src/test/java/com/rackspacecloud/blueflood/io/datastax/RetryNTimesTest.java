package com.rackspacecloud.blueflood.io.datastax;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.policies.RetryPolicy;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.junit.Assert.*;

/**
 * Unit test for RetryNTimes policy
 */
public class RetryNTimesTest {

    @Test
    public void firstTimeRetryOnReadTimeout_shouldRetry() throws Exception {
        RetryNTimes retryPolicy = new RetryNTimes(3, 3, 3);
        Statement mockStatement = mock( Statement.class );
        RetryPolicy.RetryDecision retryResult = retryPolicy.onReadTimeout(mockStatement, ConsistencyLevel.LOCAL_ONE, 1, 0, false, 0);
        RetryPolicy.RetryDecision retryExpected = RetryPolicy.RetryDecision.retry(ConsistencyLevel.LOCAL_ONE);
        assertRetryDecisionEquals(retryExpected, retryResult);
    }

    @Test
    public void maxTimeRetryOnReadTimeout_shouldRethrow() throws Exception {
        RetryNTimes retryPolicy = new RetryNTimes(3, 3, 3);
        Statement mockStatement = mock( Statement.class );

        RetryPolicy.RetryDecision retryResult = retryPolicy.onReadTimeout(mockStatement, ConsistencyLevel.LOCAL_ONE, 1, 0, false, 3);
        RetryPolicy.RetryDecision retryExpected = RetryPolicy.RetryDecision.rethrow();
        assertRetryDecisionEquals(retryExpected, retryResult);
    }

    @Test
    public void firstTimeRetryOnWriteTimeout_shouldRetry() throws Exception {
        RetryNTimes retryPolicy = new RetryNTimes(3, 3, 3);
        Statement mockStatement = mock( Statement.class );
        RetryPolicy.RetryDecision retryResult = retryPolicy.onWriteTimeout(mockStatement, ConsistencyLevel.LOCAL_ONE, WriteType.BATCH, 1, 0, 0);
        RetryPolicy.RetryDecision retryExpected = RetryPolicy.RetryDecision.retry(ConsistencyLevel.LOCAL_ONE);
        assertRetryDecisionEquals(retryExpected, retryResult);
    }

    @Test
    public void maxTimeRetryOnWriteTimeout_shouldRethrow() throws Exception {
        RetryNTimes retryPolicy = new RetryNTimes(3, 3, 3);
        Statement mockStatement = mock( Statement.class );

        RetryPolicy.RetryDecision retryResult = retryPolicy.onWriteTimeout(mockStatement, ConsistencyLevel.LOCAL_ONE, WriteType.BATCH, 1, 0, 3);
        RetryPolicy.RetryDecision retryExpected = RetryPolicy.RetryDecision.rethrow();
        assertRetryDecisionEquals(retryExpected, retryResult);
    }

    @Test
    public void firstTimeRetryOnUnavailable_shouldRetry() throws Exception {
        RetryNTimes retryPolicy = new RetryNTimes(3, 3, 3);
        Statement mockStatement = mock( Statement.class );
        RetryPolicy.RetryDecision retryResult = retryPolicy.onUnavailable(mockStatement, ConsistencyLevel.LOCAL_ONE, 1, 0, 0);
        RetryPolicy.RetryDecision retryExpected = RetryPolicy.RetryDecision.retry(ConsistencyLevel.ONE);
        assertRetryDecisionEquals(retryExpected, retryResult);
    }

    @Test
    public void maxTimeRetryOnUnavailable_shouldRethrow() throws Exception {
        RetryNTimes retryPolicy = new RetryNTimes(3, 3, 3);
        Statement mockStatement = mock( Statement.class );

        RetryPolicy.RetryDecision retryResult = retryPolicy.onUnavailable(mockStatement, ConsistencyLevel.LOCAL_ONE, 1, 0, 3);
        RetryPolicy.RetryDecision retryExpected = RetryPolicy.RetryDecision.rethrow();
        assertRetryDecisionEquals(retryExpected, retryResult);
    }

    private void assertRetryDecisionEquals(RetryPolicy.RetryDecision expected, RetryPolicy.RetryDecision result) {
        assertEquals("first time retry type", expected.getType(), result.getType());
        assertEquals("first time retry consistency level", expected.getRetryConsistencyLevel(), result.getRetryConsistencyLevel());
        assertEquals("first time retry current", expected.isRetryCurrent(), result.isRetryCurrent());
    }
}
