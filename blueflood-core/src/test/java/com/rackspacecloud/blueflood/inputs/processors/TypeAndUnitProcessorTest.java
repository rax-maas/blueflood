package com.rackspacecloud.blueflood.inputs.processors;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.MetricsCollection;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;

public class TypeAndUnitProcessorTest {
    private static final int METRICS_PER_LIST = 4;

    static MetricsCollection createTestData() {
        // create fake metrics to test
        Integer counter = 0;
        List<IMetric> l = new ArrayList<IMetric>();
        for (int j = 0; j < METRICS_PER_LIST; j++) {
            counter++;
            IMetric m = mock(Metric.class);
            l.add(m);
            // setup the metric stubs
            stub(m.getLocator()).
                toReturn(Locator.createLocatorFromDbKey(counter.toString()));
            stub(m.getCollectionTime()).toReturn(counter.longValue());
        }
        
        MetricsCollection collection = new MetricsCollection();
        collection.add(new ArrayList<IMetric>(l));
        return collection;
    }

    @Test
    public void test() throws Exception {
        MetricsCollection collection = createTestData();

        IncomingMetricMetadataAnalyzer metricMetadataAnalyzer =
            mock(IncomingMetricMetadataAnalyzer.class);

        ThreadPoolExecutor tpe = 
            new ThreadPoolBuilder().withName("rtc test").build();

        TypeAndUnitProcessor typeAndUnitProcessor = new TypeAndUnitProcessor(
                tpe, metricMetadataAnalyzer);

        typeAndUnitProcessor.apply(collection);

        // wait till done
        while (tpe.getCompletedTaskCount() < 1) {
            Thread.sleep(1);
        }

        verify(metricMetadataAnalyzer).scanMetrics(collection.toMetrics());
    }
}
