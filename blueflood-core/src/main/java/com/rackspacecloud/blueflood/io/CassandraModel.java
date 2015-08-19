package com.rackspacecloud.blueflood.io;

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.rackspacecloud.blueflood.io.serializers.LocatorSerializer;
import com.rackspacecloud.blueflood.io.serializers.SlotStateSerializer;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.SlotState;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class CassandraModel {
    public static final String KEYSPACE = Configuration.getInstance().getStringProperty(CoreConfig.ROLLUP_KEYSPACE);
    public static final String CLUSTER = Configuration.getInstance().getStringProperty(CoreConfig.CLUSTER_NAME);

    /**
    * It is worth pointing out that the actual TTL value is calculated by taking the TimeValues below
    * and multiplying by 5.  Why?  Becuase SafetyTtlProvider.java multiplies the TimeValues below by 5.
    * 
    * Look for a line like this (currently line 48):
    * TimeValue ttl = new TimeValue(metricCF.getDefaultTTL().getValue() * 5, metricCF.getDefaultTTL().getUnit());
    *
    * For example, TimeValue of 1 will equate to a 5 day TTL.
    */

    public static final MetricColumnFamily CF_METRICS_FULL = new MetricColumnFamily("metrics_full", new TimeValue(1, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_5M = new MetricColumnFamily("metrics_5m", new TimeValue(2, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_20M = new MetricColumnFamily("metrics_20m", new TimeValue(4, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_60M = new MetricColumnFamily("metrics_60m", new TimeValue(31, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_240M = new MetricColumnFamily("metrics_240m", new TimeValue(60, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_1440M = new MetricColumnFamily("metrics_1440m", new TimeValue(365, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_STRING = new MetricColumnFamily("metrics_string", new TimeValue(365 * 3, TimeUnit.DAYS));

    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_FULL = new MetricColumnFamily("metrics_preaggregated_full", new TimeValue(1, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_5M = new MetricColumnFamily("metrics_preaggregated_5m", new TimeValue(2, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_20M = new MetricColumnFamily("metrics_preaggregated_20m", new TimeValue(4, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_60M = new MetricColumnFamily("metrics_preaggregated_60m", new TimeValue(31, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_240M = new MetricColumnFamily("metrics_preaggregated_240m", new TimeValue(60, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_1440M = new MetricColumnFamily("metrics_preaggregated_1440m", new TimeValue(365, TimeUnit.DAYS));

    public static final MetricColumnFamily CF_METRICS_HIST_FULL = CF_METRICS_FULL;
    public static final MetricColumnFamily CF_METRICS_HIST_5M = new MetricColumnFamily("metrics_histogram_5m", new TimeValue(2, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_HIST_20M = new MetricColumnFamily("metrics_histogram_20m", new TimeValue(4, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_HIST_60M = new MetricColumnFamily("metrics_histogram_60m", new TimeValue(31, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_HIST_240M = new MetricColumnFamily("metrics_histogram_240m", new TimeValue(60, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_HIST_1440M = new MetricColumnFamily("metrics_histogram_1440m", new TimeValue(365, TimeUnit.DAYS));

    public static final ColumnFamily<Locator, String> CF_METRIC_METADATA = new ColumnFamily<Locator, String>("metrics_metadata",
            LocatorSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<Long, Locator> CF_METRICS_LOCATOR = new ColumnFamily<Long, Locator>("metrics_locator",
            LongSerializer.get(),
            LocatorSerializer.get());
    public static final ColumnFamily<Long, SlotState> CF_METRICS_STATE = new ColumnFamily<Long, SlotState>("metrics_state",
            LongSerializer.get(),
            SlotStateSerializer.get());

    private static final MetricColumnFamily[] METRIC_COLUMN_FAMILES = new MetricColumnFamily[] {
            CF_METRICS_FULL, CF_METRICS_5M, CF_METRICS_20M, CF_METRICS_60M, CF_METRICS_240M, CF_METRICS_1440M,
            CF_METRICS_PREAGGREGATED_FULL, CF_METRICS_PREAGGREGATED_5M, CF_METRICS_PREAGGREGATED_20M,
            CF_METRICS_PREAGGREGATED_60M, CF_METRICS_PREAGGREGATED_240M, CF_METRICS_PREAGGREGATED_1440M,
            CF_METRICS_HIST_FULL, CF_METRICS_HIST_5M, CF_METRICS_HIST_20M, CF_METRICS_HIST_60M,
            CF_METRICS_HIST_240M, CF_METRICS_HIST_1440M,
            CF_METRICS_STRING
    };

    private static final ColumnFamily[] BF_SYSTEM_COLUMN_FAMILIES = new ColumnFamily[] {
          CF_METRIC_METADATA, CF_METRICS_LOCATOR, CF_METRICS_STATE
    };

    private static final Collection<ColumnFamily> ALL_COLUMN_FAMILIES;

    private static final ColumnFamilyMapper CF_NAME_TO_CF;
    private static final ColumnFamilyMapper PREAG_GRAN_TO_CF;
    private static final ColumnFamilyMapper HIST_GRAN_TO_CF;

    private static final Map<ColumnFamily<Locator, Long>, Granularity> CF_TO_GRAN;

    static {
        final Map<Granularity, MetricColumnFamily> columnFamilyMap = new HashMap<Granularity, MetricColumnFamily>();
        columnFamilyMap.put(Granularity.FULL, CF_METRICS_FULL);
        columnFamilyMap.put(Granularity.MIN_5, CF_METRICS_5M);
        columnFamilyMap.put(Granularity.MIN_20, CF_METRICS_20M);
        columnFamilyMap.put(Granularity.MIN_60, CF_METRICS_60M);
        columnFamilyMap.put(Granularity.MIN_240, CF_METRICS_240M);
        columnFamilyMap.put(Granularity.MIN_1440, CF_METRICS_1440M);

        final Map<Granularity, MetricColumnFamily> preagCFMap = new HashMap<Granularity, MetricColumnFamily>();
        preagCFMap.put(Granularity.FULL, CF_METRICS_PREAGGREGATED_FULL);
        preagCFMap.put(Granularity.MIN_5, CF_METRICS_PREAGGREGATED_5M);
        preagCFMap.put(Granularity.MIN_20, CF_METRICS_PREAGGREGATED_20M);
        preagCFMap.put(Granularity.MIN_60, CF_METRICS_PREAGGREGATED_60M);
        preagCFMap.put(Granularity.MIN_240, CF_METRICS_PREAGGREGATED_240M);
        preagCFMap.put(Granularity.MIN_1440, CF_METRICS_PREAGGREGATED_1440M);

        final Map<Granularity, MetricColumnFamily> histCFMap = new HashMap<Granularity, MetricColumnFamily>();
        histCFMap.put(Granularity.FULL, CF_METRICS_HIST_FULL);
        histCFMap.put(Granularity.MIN_5, CF_METRICS_HIST_5M);
        histCFMap.put(Granularity.MIN_20, CF_METRICS_HIST_20M);
        histCFMap.put(Granularity.MIN_60, CF_METRICS_HIST_60M);
        histCFMap.put(Granularity.MIN_240, CF_METRICS_HIST_240M);
        histCFMap.put(Granularity.MIN_1440, CF_METRICS_HIST_1440M);

        Map<ColumnFamily<Locator, Long>, Granularity> cfToGranMap = new HashMap<ColumnFamily<Locator, Long>, Granularity>();
        cfToGranMap.put(CF_METRICS_FULL, Granularity.FULL);
        cfToGranMap.put(CF_METRICS_STRING, Granularity.FULL);
        cfToGranMap.put(CF_METRICS_5M, Granularity.MIN_5);
        cfToGranMap.put(CF_METRICS_20M, Granularity.MIN_20);
        cfToGranMap.put(CF_METRICS_60M, Granularity.MIN_60);
        cfToGranMap.put(CF_METRICS_240M, Granularity.MIN_240);
        cfToGranMap.put(CF_METRICS_1440M, Granularity.MIN_1440);

        CF_NAME_TO_CF = new ColumnFamilyMapper() {
            @Override
            public MetricColumnFamily get(Granularity gran) {
                return columnFamilyMap.get(gran);
            }
        };
        PREAG_GRAN_TO_CF = new ColumnFamilyMapper() {
            @Override
            public MetricColumnFamily get(Granularity gran) {
                return preagCFMap.get(gran);
            }
        };
        HIST_GRAN_TO_CF = new ColumnFamilyMapper() {
            @Override
            public MetricColumnFamily get(Granularity gran) {
                return histCFMap.get(gran);
            }
        };
        CF_TO_GRAN = Collections.unmodifiableMap(cfToGranMap);
        List<ColumnFamily> cfs = new ArrayList<ColumnFamily>();

        for (ColumnFamily cf : METRIC_COLUMN_FAMILES) {
            cfs.add(cf);
        }

        for (ColumnFamily cf : BF_SYSTEM_COLUMN_FAMILIES) {
            cfs.add(cf);
        }
        ALL_COLUMN_FAMILIES = Collections.unmodifiableList(cfs);
    }

    public static ColumnFamily getColumnFamily(Class<? extends Rollup> type, Granularity granularity) {
        if (type.equals(SimpleNumber.class)) {
            return CF_METRICS_FULL;
        } else if (type.equals(BasicRollup.class)) {
            return CF_NAME_TO_CF.get(granularity);
        } else if (type.equals(HistogramRollup.class)) {
            return HIST_GRAN_TO_CF.get(granularity);
        } else if (type.equals(BluefloodSetRollup.class) || type.equals(BluefloodTimerRollup.class) || type.equals(BluefloodGaugeRollup.class) ||
                type.equals(BluefloodCounterRollup.class)) {
            return PREAG_GRAN_TO_CF.get(granularity);
        } else {
            throw new RuntimeException("Unsupported rollup type.");
        }
    }

    public static ColumnFamily getColumnFamily(RollupType type, DataType dataType, Granularity gran) {
        if (dataType == null) {
            dataType = DataType.NUMERIC;
        }

        if (type == null) {
            type = RollupType.BF_BASIC;
        }

        if (type == RollupType.BF_BASIC && (dataType.equals(DataType.BOOLEAN) || dataType.equals(DataType.STRING))) {
            return CF_METRICS_STRING;
        }

        return getColumnFamily(RollupType.classOf(type, gran), gran);
    }

    // iterate over all column families that store metrics.
    public static Iterable<MetricColumnFamily> getMetricColumnFamilies() {
        return new Iterable<MetricColumnFamily>() {
            @Override
            public Iterator<MetricColumnFamily> iterator() {
                return new Iterator<MetricColumnFamily>() {
                    private int pos = 0;
                    @Override
                    public boolean hasNext() {
                        return pos < METRIC_COLUMN_FAMILES.length;
                    }

                    @Override
                    public MetricColumnFamily next() {
                        return METRIC_COLUMN_FAMILES[pos++];
                    }

                    @Override
                    public void remove() {
                        throw new NoSuchMethodError("Not implemented");
                    }
                };
            }
        };
    }

    public static Collection<ColumnFamily> getAllColumnFamilies() {
        return ALL_COLUMN_FAMILIES;
    }

    public static class MetricColumnFamily extends ColumnFamily<Locator, Long>  {
        private final TimeValue ttl;

        public MetricColumnFamily(String name, TimeValue ttl) {
            super(name, LocatorSerializer.get(), LongSerializer.get());
            this.ttl = ttl;
        }

        public TimeValue getDefaultTTL() {
            return ttl;
        }
    }

    // future versions will have get(Granularity, RollupType).
    public interface ColumnFamilyMapper {
        public MetricColumnFamily get(Granularity gran);
    }
}
