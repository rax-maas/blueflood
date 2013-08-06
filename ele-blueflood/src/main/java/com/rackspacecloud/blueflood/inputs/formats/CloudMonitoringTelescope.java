package com.rackspacecloud.blueflood.inputs.formats;

import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.types.ServerMetricLocator;
import com.rackspacecloud.blueflood.utils.Util;
import com.rackspacecloud.blueflood.utils.MetricHelper;
import com.rackspacecloud.blueflood.utils.TimeValue;
import com.google.common.base.Strings;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import scribe.thrift.LogEntry;
import telescope.thrift.Telescope;
import telescope.thrift.TelescopeOrRemove;
import telescope.thrift.UnitEnum;

import javax.xml.bind.DatatypeConverter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CloudMonitoringTelescope extends MetricsContainer {
    public Telescope telescope;

    private static final TimeValue DEFAULT_TTL = new TimeValue(2, TimeUnit.DAYS);
    private static final int DOUBLE = (int) MetricHelper.Type.DOUBLE;
    private static final int I32 = (int) MetricHelper.Type.INT32;
    private static final int U32 = (int) MetricHelper.Type.UINT32;
    private static final int I64 = (int) MetricHelper.Type.INT64;
    private static final int U64 = (int) MetricHelper.Type.UINT64;
    private static final int STR = (int) MetricHelper.Type.STRING;
    private static final int BOOL = (int) MetricHelper.Type.BOOLEAN;

    public CloudMonitoringTelescope(LogEntry message) throws TException, IllegalArgumentException {
        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory(false, false));
        byte[] decodedBytes = DatatypeConverter.parseBase64Binary(message.message);
        TelescopeOrRemove tscope = new TelescopeOrRemove();
        deserializer.deserialize(tscope, decodedBytes);

        if (!tscope.isSetTelescope()) {
            throw new RuntimeException("Message does not contain a telescope");
        }

        if (!isValid(tscope.getTelescope())) {
            throw new IllegalArgumentException("Invalid telescope " + asString(telescope));
        }

        this.telescope = tscope.getTelescope();
    }

    public CloudMonitoringTelescope(Telescope telescope) {
        if (!isValid(telescope)) {
            throw new IllegalArgumentException("Invalid telescope " + asString(telescope));
        }
        this.telescope = telescope;
    }

    @Override
    public List<Metric> toMetrics() {
        final Map<String, telescope.thrift.Metric> telMetrics = telescope.getMetrics();

        if (telMetrics == null) {
            return null;
        }

        final String accountId = telescope.getAcctId();
        final String entityId = telescope.getEntityId();
        final String checkId = telescope.getCheckId();

        final List<com.rackspacecloud.blueflood.types.Metric> metrics = new ArrayList<Metric>();
        for (Map.Entry<String, telescope.thrift.Metric> telMetric : telMetrics.entrySet()) {
            final Locator locator = ServerMetricLocator.createFromTelescopePrimitives(accountId, entityId,
                    checkId, Util.generateMetricName(telMetric.getKey(), telescope.getMonitoringZoneId()));
            final com.rackspacecloud.blueflood.types.Metric metric = new com.rackspacecloud.blueflood.types.Metric(locator,
                    getMetricValue(telMetric.getValue()), telescope.getTimestamp(), DEFAULT_TTL,
                    getMetricUnitString(telMetric.getValue()));
            metrics.add(metric);
        }

        return metrics;
    }

    public Telescope getTelescope() {
        return telescope;
    }

    public static String asString(Telescope telescope) {
        return String.format("acct: %s, ent:%s, chck:%s, mz:%s",
                Strings.nullToEmpty(telescope.getAcctId()),
                Strings.nullToEmpty(telescope.getEntityId()),
                Strings.nullToEmpty(telescope.getCheckId()),
                Strings.nullToEmpty(telescope.getMonitoringZoneId()));
    }

    public String toString() {
        return asString(this.telescope);
    }

    public static Object getMetricValue(telescope.thrift.Metric m) {
        switch (m.getMetricType()) {
            case DOUBLE: return m.getValueDbl();
            case I32: return m.getValueI32();
            case U32: return (long) m.getValueI32() & 0xffffffffL;
            case I64: return m.getValueI64();
            case U64: return m.getValueI64(); // this value may be negative. should use big integers?
            case STR: return m.getValueStr();
            case BOOL: return m.isValueBool();
            default: throw new RuntimeException("Unexpected metric type: " + (char)m.getMetricType());
        }
    }

    public static String getMetricUnitString(telescope.thrift.Metric m) {
        if (m.getUnitEnum() == null) {
            return (UnitEnum.UNKNOWN.name().toLowerCase());
        } else if (m.getUnitEnum() == UnitEnum.OTHER) {
            return (m.getUnitOtherStr() == null ? UnitEnum.UNKNOWN.name().toLowerCase() : m.getUnitOtherStr());
        }
        return m.getUnitEnum().name().toLowerCase();
    }

    private static boolean isValid(Telescope tel) {
        if (Strings.isNullOrEmpty(tel.getAcctId())) {
            return false;
        } else if (Strings.isNullOrEmpty(tel.getEntityId())) {
            return false;
        } else if (Strings.isNullOrEmpty(tel.getCheckId())) {
            return false;
        }

        return true;
    }
}
