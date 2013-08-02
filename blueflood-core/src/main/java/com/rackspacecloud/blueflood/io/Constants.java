package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.utils.MetricHelper;
import org.jboss.netty.util.CharsetUtil;

import java.nio.charset.Charset;

public class Constants {
    
    public static final int VERSION_FIELD_OFFSET = 0;

    public static final byte VERSION_1_FULL_RES = 0;
    public static final byte VERSION_1_ROLLUP = 0;

    public static final int DOUBLE = (int) MetricHelper.Type.DOUBLE;
    public static final int I32 = (int) MetricHelper.Type.INT32;
    public static final int I64 = (int) MetricHelper.Type.INT64;
    public static final int STR = (int) MetricHelper.Type.STRING;
 
    public static final byte B_DOUBLE = (byte)DOUBLE;
    public static final byte B_I32 = (byte)I32;
    public static final byte B_I64 = (byte)I64;
    public static final byte B_STR = (byte)STR;

    public static final byte AVERAGE = 0;
    public static final byte VARIANCE = 1;
    public static final byte MIN = 2;
    public static final byte MAX = 3;

    public static final String monitoringZonePrefix = "mz";
    
    public static final int NUMBER_OF_SHARDS = 128;

    // ensure that some yahoo did not set FullResSerializer.CUR_VERSION to an invalid value (for backwards compatibility
    // with old unversioned serializations).
    static {
        if (VERSION_1_FULL_RES == DOUBLE || VERSION_1_FULL_RES == I32 || VERSION_1_FULL_RES == I64 || VERSION_1_FULL_RES == STR)
            throw new RuntimeException("Invalid FullResSerializer.CUR_VERSION. Please increment until this exception does not happen.");
    }

    public static final Charset DEFAULT_CHARSET = CharsetUtil.UTF_8;

    private Constants() {}
}
