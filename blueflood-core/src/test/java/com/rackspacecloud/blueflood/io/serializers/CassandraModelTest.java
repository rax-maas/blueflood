package com.rackspacecloud.blueflood.io.serializers;

import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.CassandraModel.MetricColumnFamily;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.RollupType;
import junit.framework.Assert;
import org.junit.Test;


public class CassandraModelTest {

    @Test
    public void test_getColumnFamily_RetrievesNumericCF_ForNullMetadata() {
        RollupType nullType = null;
        MetricColumnFamily cf = CassandraModel.getColumnFamily(nullType, Granularity.FULL);
        Assert.assertTrue(cf != null);
        Assert.assertTrue(cf.getName().equals(CassandraModel.CF_METRICS_FULL.getName()));

        cf = CassandraModel.getColumnFamily(nullType, Granularity.MIN_5);
        Assert.assertTrue(cf != null);
        Assert.assertTrue(cf.getName().equals(CassandraModel.CF_METRICS_5M.getName()));

        cf = CassandraModel.getColumnFamily(nullType, Granularity.MIN_20);
        Assert.assertTrue(cf != null);
        Assert.assertTrue(cf.getName().equals(CassandraModel.CF_METRICS_20M.getName()));

        cf = CassandraModel.getColumnFamily(nullType, Granularity.MIN_60);
        Assert.assertTrue(cf != null);
        Assert.assertTrue(cf.getName().equals(CassandraModel.CF_METRICS_60M.getName()));

        cf = CassandraModel.getColumnFamily(nullType, Granularity.MIN_240);
        Assert.assertTrue(cf != null);
        Assert.assertTrue(cf.getName().equals(CassandraModel.CF_METRICS_240M.getName()));

        cf = CassandraModel.getColumnFamily(nullType, Granularity.MIN_1440);
        Assert.assertTrue(cf != null);
        Assert.assertTrue(cf.getName().equals(CassandraModel.CF_METRICS_1440M.getName()));
    }

    @Test
    public void test_getColumnFamily_Retrieves_CounterCF_ForNullDataType_ForCounters() {
        MetricColumnFamily cf = CassandraModel.getColumnFamily(RollupType.COUNTER, Granularity.FULL);
        Assert.assertTrue(cf != null);
        Assert.assertTrue(cf.getName().equals(CassandraModel.CF_METRICS_PREAGGREGATED_FULL.getName()));
    }
}
