/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.types;


import com.rackspacecloud.blueflood.exceptions.InvalidDataException;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;

public class Metric implements IMetric {
    private static final Logger log = LoggerFactory.getLogger(Metric.class);

    private final Locator locator;
    private Object metricValue;
    private final long collectionTime;
    private int ttlInSeconds;
    private DataType dataType;
    private final String unit;
    private static BigDecimal DOUBLE_MAX = new BigDecimal(Double.MAX_VALUE);

    public Metric(Locator locator, Object metricValue, long collectionTime, TimeValue ttl, String unit) {
        this.locator = locator;
        this.metricValue = metricValue;
        // I dislike throwing errors in constructors, but there is no other way without resorting to a json schema.
        if (collectionTime < 0) {
            throw new InvalidDataException("collection time must be greater than zero");
        }
        this.collectionTime = collectionTime;
        this.dataType = DataType.getMetricType(metricValue);
        this.unit = unit;

        // TODO: Until we start handling BigInteger throughout, let's try to cast it to double if the int value is less
        // than Double.MAX_VALUE

        if (metricValue instanceof BigInteger) {
            BigDecimal maybeDouble = new BigDecimal((BigInteger) metricValue);
            if (maybeDouble.compareTo(DOUBLE_MAX) > 0) {
                log.warn("BigInteger metric value " + ((BigInteger)metricValue).toString() + " for metric "
                        + locator.toString() + " is bigger than Double.MAX_VALUE");
                throw new RuntimeException("BigInteger cannot be force cast to double as it exceeds Double.MAX_VALUE");
            }
            this.dataType = DataType.NUMERIC;
            this.metricValue = ((BigInteger) metricValue).doubleValue();
        }

        setTtl(ttl);
    }

    public Locator getLocator() {
        return locator;
    }

    public Object getMetricValue() {
        return metricValue;
    }

    public DataType getDataType() {
        return dataType;
    }

    public int getTtlInSeconds() {
        return ttlInSeconds;
    }

    public long getCollectionTime() {
        return collectionTime;
    }

    public String getUnit() {
        return unit;
    }

    public boolean isNumeric() {
        return DataType.isNumericMetric(metricValue);
    }

    public boolean isString() {
        return DataType.isStringMetric(metricValue);
    }

    public boolean isBoolean() {
        return DataType.isBooleanMetric(metricValue);
    }

    public void setTtl(TimeValue ttl) {
        if (!isValidTTL(ttl.toSeconds())) {
            throw new InvalidDataException("TTL supplied for metric is invalid. Required: 0 < ttl < " + Integer.MAX_VALUE +
                    ", provided: " + ttl.toSeconds());
        }

        ttlInSeconds = (int) ttl.toSeconds();
    }

    public void setTtlInSeconds(int ttlInSeconds) {
        if (!isValidTTL(ttlInSeconds)) {
            throw new InvalidDataException("TTL supplied for metric is invalid. Required: 0 < ttl < " + Integer.MAX_VALUE +
                    ", provided: " + ttlInSeconds);
        }

        this.ttlInSeconds = ttlInSeconds;
    }
    
    public RollupType getRollupType() {
        return RollupType.BF_BASIC;
    }

    @Override
    public String toString() {
        return String.format("%s:%s:%s:%s:%s", locator.toString(), metricValue, dataType, ttlInSeconds, unit == null ? "" : unit.toString());
    }

    private boolean isValidTTL(long ttlInSeconds) {
        return (ttlInSeconds < Integer.MAX_VALUE && ttlInSeconds > 0);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Metric)) {
            return false;
        }
        Metric other = (Metric) o;
        if (locator.equals(other.getLocator()) &&
                collectionTime == other.getCollectionTime() &&
                ttlInSeconds == other.getTtlInSeconds() &&
                dataType.equals(other.getDataType()) &&
                unit.equals(other.getUnit())) {
            return true;
        }
        return false;
    }
}
