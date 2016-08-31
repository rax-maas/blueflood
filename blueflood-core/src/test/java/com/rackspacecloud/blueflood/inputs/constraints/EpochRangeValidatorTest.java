package com.rackspacecloud.blueflood.inputs.constraints;

import com.rackspacecloud.blueflood.inputs.formats.JSONMetric;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import org.junit.Before;
import org.junit.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class EpochRangeValidatorTest {

    private Validator validator;
    private JSONMetric metric = new JSONMetric();

    @Before
    public void setup() {
        metric.setMetricName("a.b.c.d");
        metric.setTtlInSeconds(1);
        metric.setMetricValue(1);

        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    public void testCollectionTimeValid() {
        long currentTime = new DefaultClockImpl().now().getMillis();

        metric.setCollectionTime(currentTime);

        Set<ConstraintViolation<JSONMetric>> constraintViolations = validator.validate(metric);
        assertEquals(0, constraintViolations.size());
    }

    @Test
    public void testCollectionTimeInPast() {
        long currentTime = new DefaultClockImpl().now().getMillis();
        long collectionTimeInPast = currentTime - EpochRangeLimits.BEFORE_CURRENT_TIME_MS.getValue() - 1000;

        metric.setCollectionTime(collectionTimeInPast);

        Set<ConstraintViolation<JSONMetric>> constraintViolations = validator.validate(metric);
        assertEquals(1, constraintViolations.size());
        assertEquals("collectionTime", constraintViolations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testCollectionTimeInFuture() {
        long currentTime = new DefaultClockImpl().now().getMillis();
        long collectionTimeInFuture = currentTime + EpochRangeLimits.AFTER_CURRENT_TIME_MS.getValue() + 1000;

        metric.setCollectionTime(collectionTimeInFuture);

        Set<ConstraintViolation<JSONMetric>> constraintViolations = validator.validate(metric);
        assertEquals(1, constraintViolations.size());
        assertEquals("collectionTime", constraintViolations.iterator().next().getPropertyPath().toString());
    }
}
