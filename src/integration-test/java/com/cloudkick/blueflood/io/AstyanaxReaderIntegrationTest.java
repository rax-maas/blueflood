package com.cloudkick.blueflood.io;

import com.cloudkick.blueflood.rollup.Granularity;
import com.cloudkick.blueflood.types.Locator;
import com.cloudkick.blueflood.types.Metric;
import com.cloudkick.blueflood.types.Range;
import com.cloudkick.blueflood.types.ServerMetricLocator;
import org.junit.Assert;
import org.junit.Test;
import telescope.thrift.RollupMetric;

import java.util.List;

public class AstyanaxReaderIntegrationTest extends IntegrationTestBase {
    
    @Test
    public void testCanReadNumeric() throws Exception {
        Metric metric = writeMetric("long_metric", 74L);
        AstyanaxReader reader = AstyanaxReader.getInstance();

        final Locator locator = metric.getLocator();
        List<RollupMetric> res = reader.getDatapointsForRange(locator, new Range(metric.getCollectionTime() - 100000,
                metric.getCollectionTime() + 100000), Granularity.FULL);
        int numPoints = res.size();
        assertTrue(numPoints > 0);

        // Test that the RangeBuilder is end-inclusive on the timestamp.
        res = reader.getDatapointsForRange(locator, new Range(metric.getCollectionTime() - 100000,
                metric.getCollectionTime()), Granularity.FULL);
        assertEquals(numPoints, res.size());
    }
    
    @Test
    public void testCanReadString() throws Exception {
        Metric metric = writeMetric("string_metric", "version 1.0.43342346");
        final Locator locator = metric.getLocator();

        AstyanaxReader reader = AstyanaxReader.getInstance();
        List<RollupMetric> res = reader.getDatapointsForRange(locator, new Range(metric.getCollectionTime() - 100000,
                metric.getCollectionTime() + 100000), Granularity.FULL);
        assertTrue(res.size() > 0);
    }
    
    @Test
    public void testCanReadMetadata() throws Exception {
        ServerMetricLocator loc1 = ServerMetricLocator.createFromTelescopePrimitives("acOne", "ent", "ch", "mz.met");
        AstyanaxWriter writer = AstyanaxWriter.getInstance();
        AstyanaxReader reader = AstyanaxReader.getInstance();
// TODO: uncomment the following pending https://issues.rax.io/browse/CMD-139
//        Map<String, byte[]> expected = new HashMap<String, byte[]>(){{
//            put("a", new byte[]{1,2,3,4,5});
//            put("b", new byte[]{6,7,8,9,0});
//            put("c", new byte[]{11,22,33,44,55,66,77,88});
//            put("d", new byte[]{-1,-2,-3,-4});
//        }};
        
//        for (Map.Entry<String, byte[]> entry : expected.entrySet()) {
//            writer.writeMetadataValue(loc1, entry.getKey(), entry.getValue());
//        }

//        for (Map.Entry<String, byte[]> entry : expected.entrySet()) {
//            Assert.assertArrayEquals(
//                "broke on " + entry.getKey(),
//                entry.getValue(),
//                (byte[])reader.getMetadataValue(loc1, entry.getKey())
//            );
//        }
        writer.writeMetadataValue(loc1, "foo", "bar");
        Assert.assertEquals("bar", reader.getMetadataValue(loc1, "foo").toString());
    }
}
