package com.rackspacecloud.blueflood.rollup;

import com.rackspacecloud.blueflood.types.Metric;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MetricsPersistenceOptimizerFactoryTest {

    @Test
    public void testFactoryGetOptimizer() {
        MetricsPersistenceOptimizer optimizer =
                MetricsPersistenceOptimizerFactory.getOptimizer(Metric.Type.STRING);

        // we should get a valid optimizer
        assertEquals(false, optimizer == null);
        // we should get a StringMetricsPersistenceOptimizer
        assertEquals(true, optimizer instanceof StringMetricsPersistenceOptimizer);

        optimizer = MetricsPersistenceOptimizerFactory.getOptimizer(Metric.Type.DOUBLE);

        // we should set a GenericMetricsPersistenceOptimizer
        assertEquals(true, optimizer instanceof GenericMetricsPersistenceOptimizer);
    }
}