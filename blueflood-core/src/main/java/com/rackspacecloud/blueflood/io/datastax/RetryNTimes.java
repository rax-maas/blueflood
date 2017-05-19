package com.rackspacecloud.blueflood.io.datastax;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that retry on read/write/unavailable timeouts n times, where
 * n is a configurable number
 */
public class RetryNTimes implements RetryPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(RetryNTimes.class);
    private final int readAttempts;
    private final int writeAttempts;
    private final int unavailableAttempts;

    /**
     * A Datastax {@link RetryPolicy} that is capable of retrying n times
     * @param readAttempts
     * @param writeAttempts
     * @param unavailableAttempts
     */
    public RetryNTimes(int readAttempts, int writeAttempts, int unavailableAttempts) {
        this.readAttempts = readAttempts;
        this.writeAttempts = writeAttempts;
        this.unavailableAttempts = unavailableAttempts;
    }

    @Override
    public RetryDecision onReadTimeout(Statement stmnt, ConsistencyLevel cl,
                                       int requiredResponses, int receivedResponses,
                                       boolean dataReceived, int rTime) {
        if (dataReceived) {
            return RetryDecision.ignore();
        } else if (rTime < readAttempts) {
            LOG.info(String.format("Retrying on ReadTimeout: stmnt %s, " +
                                   "consistency %s, requiredResponse %d, " +
                                   "receivedResponse %d, dataReceived %s, rTime %d",
                            stmnt, cl, requiredResponses, receivedResponses, dataReceived, rTime));
            return RetryDecision.retry(cl);
        } else {
            return RetryDecision.rethrow();
        }

    }

    @Override
    public RetryDecision onWriteTimeout(Statement stmnt, ConsistencyLevel cl,
                                        WriteType wt, int requiredResponses,
                                        int receivedResponses, int wTime) {
        if (wTime < writeAttempts) {
            LOG.info(String.format("Retrying on WriteTimeout: stmnt %s, " +
                                   "consistency %s, writeType %s, requiredResponse %d, " +
                                   "receivedResponse %d, rTime %d",
                    stmnt, cl, wt.toString(), requiredResponses, receivedResponses, wTime));
            return RetryDecision.retry(cl);
        }
        return RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onUnavailable(Statement stmnt, ConsistencyLevel cl,
                                       int requiredResponses, int receivedResponses, int uTime) {
        if (uTime < unavailableAttempts) {
            LOG.info(String.format("Retrying on unavailable: stmnt %s, consistency %s, " +
                                   "requiredResponse %d, receivedResponse %d, rTime %d",
                    stmnt, cl, requiredResponses, receivedResponses, uTime));
            return RetryDecision.retry(ConsistencyLevel.ONE);
        }
        return RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
        LOG.info(String.format("Trying nextHost on requestError: stmnt %s, consistency %s, driver ex %s, nbRetry %d",
                statement, cl, e, nbRetry));
        return RetryDecision.tryNextHost(cl);
    }

    @Override
    public void init(Cluster cluster) {
    }

    @Override
    public void close() {
    }
}
