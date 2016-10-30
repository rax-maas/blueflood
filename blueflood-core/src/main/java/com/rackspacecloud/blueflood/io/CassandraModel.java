package com.rackspacecloud.blueflood.io;

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.rackspacecloud.blueflood.io.serializers.astyanax.LocatorSerializer;
import com.rackspacecloud.blueflood.io.serializers.astyanax.SlotKeySerializer;
import com.rackspacecloud.blueflood.io.serializers.astyanax.SlotStateSerializer;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.SlotState;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class CassandraModel {
    public static final String KEYSPACE = Configuration.getInstance().getStringProperty(CoreConfig.ROLLUP_KEYSPACE);
    public static final String QUOTED_KEYSPACE = "\"" + KEYSPACE + "\"";
    public static final String CLUSTER = Configuration.getInstance().getStringProperty(CoreConfig.CLUSTER_NAME);

    /**
     * The following CF_*_NAME are the names of all of our Column Family
     */
    public static final String CF_METRICS_STATE_NAME = "metrics_state";
    public static final String CF_METRICS_METADATA_NAME = "metrics_metadata";
    public static final String CF_METRICS_LOCATOR_NAME = "metrics_locator";
    public static final String CF_METRICS_DELAYED_LOCATOR_NAME = "metrics_delayed_locator";
    public static final String CF_METRICS_STRING_NAME = "metrics_string";
    public static final String CF_METRICS_ENUM_NAME = "metrics_enum";
    public static final String CF_METRICS_EXCESS_ENUMS_NAME = "metrics_excess_enums";

    public static final String CF_METRICS_FULL_NAME = "metrics_full";
    public static final String CF_METRICS_5M_NAME = "metrics_5m";
    public static final String CF_METRICS_20M_NAME = "metrics_20m";
    public static final String CF_METRICS_60M_NAME = "metrics_60m";
    public static final String CF_METRICS_240M_NAME = "metrics_240m";
    public static final String CF_METRICS_1440M_NAME = "metrics_1440m";

    public static final String CF_METRICS_PREAGGREGATED_FULL_NAME = "metrics_preaggregated_full";
    public static final String CF_METRICS_PREAGGREGATED_5M_NAME = "metrics_preaggregated_5m";
    public static final String CF_METRICS_PREAGGREGATED_20M_NAME = "metrics_preaggregated_20m";
    public static final String CF_METRICS_PREAGGREGATED_60M_NAME = "metrics_preaggregated_60m";
    public static final String CF_METRICS_PREAGGREGATED_240M_NAME = "metrics_preaggregated_240m";
    public static final String CF_METRICS_PREAGGREGATED_1440M_NAME = "metrics_preaggregated_1440m";

    public static final MetricColumnFamily CF_METRICS_FULL = new MetricColumnFamily(CF_METRICS_FULL_NAME, new TimeValue(5, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_5M = new MetricColumnFamily(CF_METRICS_5M_NAME, new TimeValue(10, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_20M = new MetricColumnFamily(CF_METRICS_20M_NAME, new TimeValue(20, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_60M = new MetricColumnFamily(CF_METRICS_60M_NAME, new TimeValue(31 * 5, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_240M = new MetricColumnFamily(CF_METRICS_240M_NAME, new TimeValue(60 * 5, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_1440M = new MetricColumnFamily(CF_METRICS_1440M_NAME, new TimeValue(365 * 5, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_STRING = new MetricColumnFamily(CF_METRICS_STRING_NAME, new TimeValue(365 * 3 * 5, TimeUnit.DAYS));

    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_FULL = new MetricColumnFamily(CF_METRICS_PREAGGREGATED_FULL_NAME, new TimeValue(5, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_5M = new MetricColumnFamily(CF_METRICS_PREAGGREGATED_5M_NAME, new TimeValue(10, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_20M = new MetricColumnFamily(CF_METRICS_PREAGGREGATED_20M_NAME, new TimeValue(20, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_60M = new MetricColumnFamily(CF_METRICS_PREAGGREGATED_60M_NAME, new TimeValue(31 * 5, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_240M = new MetricColumnFamily(CF_METRICS_PREAGGREGATED_240M_NAME, new TimeValue(60 * 5, TimeUnit.DAYS));
    public static final MetricColumnFamily CF_METRICS_PREAGGREGATED_1440M = new MetricColumnFamily(CF_METRICS_PREAGGREGATED_1440M_NAME, new TimeValue(365 * 5, TimeUnit.DAYS));

    public static final ColumnFamily<Locator, String> CF_METRICS_METADATA = new ColumnFamily<Locator, String>(CF_METRICS_METADATA_NAME,
            LocatorSerializer.get(),
            StringSerializer.get());

    public static final ColumnFamily<Locator, Long> CF_METRICS_ENUM = new ColumnFamily<Locator, Long>(CF_METRICS_ENUM_NAME,
            LocatorSerializer.get(),
            LongSerializer.get(),
            StringSerializer.get());

    public static final ColumnFamily<Long, Locator> CF_METRICS_LOCATOR = new ColumnFamily<Long, Locator>(CF_METRICS_LOCATOR_NAME,
            LongSerializer.get(),
            LocatorSerializer.get());

    public static final ColumnFamily<SlotKey, Locator> CF_METRICS_DELAYED_LOCATOR = new ColumnFamily<SlotKey,
            Locator>(CF_METRICS_DELAYED_LOCATOR_NAME,
            SlotKeySerializer.get(),
            LocatorSerializer.get());

    public static final ColumnFamily<Long, SlotState> CF_METRICS_STATE = new ColumnFamily<Long, SlotState>(CF_METRICS_STATE_NAME,
            LongSerializer.get(),
            SlotStateSerializer.get());

    public static final ColumnFamily<Locator, Long> CF_METRICS_EXCESS_ENUMS = new ColumnFamily<Locator, Long>(CF_METRICS_EXCESS_ENUMS_NAME,
            LocatorSerializer.get(),
            LongSerializer.get());

    private static final MetricColumnFamily[] METRIC_COLUMN_FAMILES = new MetricColumnFamily[] {
            CF_METRICS_FULL, CF_METRICS_5M, CF_METRICS_20M, CF_METRICS_60M, CF_METRICS_240M, CF_METRICS_1440M,
            CF_METRICS_PREAGGREGATED_FULL, CF_METRICS_PREAGGREGATED_5M, CF_METRICS_PREAGGREGATED_20M,
            CF_METRICS_PREAGGREGATED_60M, CF_METRICS_PREAGGREGATED_240M, CF_METRICS_PREAGGREGATED_1440M,
            CF_METRICS_STRING
    };

    private static final ColumnFamily[] BF_SYSTEM_COLUMN_FAMILIES = new ColumnFamily[] {
            CF_METRICS_METADATA, CF_METRICS_LOCATOR, CF_METRICS_DELAYED_LOCATOR, CF_METRICS_STATE, CF_METRICS_EXCESS_ENUMS
    };

    private static final Collection<ColumnFamily> ALL_COLUMN_FAMILIES;

    private static final ColumnFamilyMapper METRICS_GRAN_TO_CF;
    private static final ColumnFamilyMapper PREAG_GRAN_TO_CF;

    private static final Map<String, ColumnFamily<Locator, Long>> CF_NAME_TO_CF = new HashMap<String, ColumnFamily<Locator, Long>>();
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

        Map<ColumnFamily<Locator, Long>, Granularity> cfToGranMap = new HashMap<ColumnFamily<Locator, Long>, Granularity>();
        cfToGranMap.put(CF_METRICS_FULL, Granularity.FULL);
        cfToGranMap.put(CF_METRICS_STRING, Granularity.FULL);
        cfToGranMap.put(CF_METRICS_5M, Granularity.MIN_5);
        cfToGranMap.put(CF_METRICS_20M, Granularity.MIN_20);
        cfToGranMap.put(CF_METRICS_60M, Granularity.MIN_60);
        cfToGranMap.put(CF_METRICS_240M, Granularity.MIN_240);
        cfToGranMap.put(CF_METRICS_1440M, Granularity.MIN_1440);
        cfToGranMap.put(CF_METRICS_PREAGGREGATED_FULL, Granularity.FULL);
        cfToGranMap.put(CF_METRICS_PREAGGREGATED_5M, Granularity.MIN_5);
        cfToGranMap.put(CF_METRICS_PREAGGREGATED_20M, Granularity.MIN_20);
        cfToGranMap.put(CF_METRICS_PREAGGREGATED_60M, Granularity.MIN_60);
        cfToGranMap.put(CF_METRICS_PREAGGREGATED_240M, Granularity.MIN_240);
        cfToGranMap.put(CF_METRICS_PREAGGREGATED_1440M, Granularity.MIN_1440);

        METRICS_GRAN_TO_CF = new ColumnFamilyMapper() {
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
        CF_TO_GRAN = Collections.unmodifiableMap(cfToGranMap);
        List<ColumnFamily> cfs = new ArrayList<ColumnFamily>();

        for (ColumnFamily cf : METRIC_COLUMN_FAMILES) {
            cfs.add(cf);
        }

        for (ColumnFamily cf : BF_SYSTEM_COLUMN_FAMILIES) {
            cfs.add(cf);
        }
        ALL_COLUMN_FAMILIES = Collections.unmodifiableList(cfs);

        for ( ColumnFamily cf : ALL_COLUMN_FAMILIES) {
            CF_NAME_TO_CF.put(cf.getName(), cf);
        }
    }

    public static MetricColumnFamily getColumnFamily(Class<? extends Rollup> type, Granularity granularity) {
        if (type.equals(SimpleNumber.class)) {
            return CF_METRICS_FULL;
        } else if (type.equals(BasicRollup.class)) {
            return METRICS_GRAN_TO_CF.get(granularity);
        } else if (type.equals(BluefloodSetRollup.class) || type.equals(BluefloodTimerRollup.class) || type.equals(BluefloodGaugeRollup.class) ||
                type.equals(BluefloodCounterRollup.class) || type.equals(BluefloodEnumRollup.class)) {
            return PREAG_GRAN_TO_CF.get(granularity);
        } else {
            throw new RuntimeException("Unsupported rollup type.");
        }
    }

    public static MetricColumnFamily getColumnFamily(RollupType type, DataType dataType, Granularity gran) {
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

    public static MetricColumnFamily getPreaggregatedColumnFamily(Granularity gran) {
        return PREAG_GRAN_TO_CF.get(gran);
    }

    public static String getPreaggregatedColumnFamilyName(Granularity gran) {
        return PREAG_GRAN_TO_CF.get(gran).getName();
    }

    public static MetricColumnFamily getBasicColumnFamily(Granularity gran) {
        return METRICS_GRAN_TO_CF.get(gran);
    }

    public static String getBasicColumnFamilyName(Granularity gran) {
        return METRICS_GRAN_TO_CF.get(gran).getName();
    }

    public static ColumnFamily getColumnFamily(String columnFamilyName) {
        return CF_NAME_TO_CF.get(columnFamilyName);
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

    public static Granularity getGranularity(ColumnFamily cf) {
        return CF_TO_GRAN.get(cf);
    }

    public static Granularity getGranularity(String cf) {
        return CF_TO_GRAN.get(CF_NAME_TO_CF.get(cf));
    }

    public static Collection<ColumnFamily> getAllColumnFamilies() {
        return ALL_COLUMN_FAMILIES;
    }

    public static Collection<String> getAllColumnFamiliesNames() {

        List<String> names = new ArrayList<String>() {{
                for (ColumnFamily cf : ALL_COLUMN_FAMILIES) {
                    add(cf.getName());
                }
        }};
        return names;
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

