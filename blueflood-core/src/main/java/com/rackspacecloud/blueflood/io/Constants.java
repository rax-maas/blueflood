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

package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.utils.MetricHelper;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.jboss.netty.util.CharsetUtil;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

public class Constants {
    
    public static final int VERSION_FIELD_OFFSET = 0;

    public static final byte VERSION_1_FULL_RES = 0;
    public static final byte VERSION_1_ROLLUP = 0;
    public static final byte VERSION_1_HISTOGRAM = 0;
    public static final byte VERSION_1_TIMER = 0;
    public static final byte VERSION_2_TIMER = 1;

    public static final byte VERSION_1_COUNTER_ROLLUP = 0;
    public static final byte VERSION_1_SET_ROLLUP = VERSION_1_ROLLUP; // don't change this.

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

    public static final TimeValue STRING_SAFETY_TTL = new TimeValue(365, TimeUnit.DAYS);

    public static final int NUMBER_OF_SHARDS = 128;
    
    public static final int DEFAULT_SAMPLE_INTERVAL = 30; // seconds.

    // ensure that some yahoo did not set FullResSerializer.CUR_VERSION to an invalid value (for backwards compatibility
    // with old unversioned serializations).
    static {
        if (VERSION_1_FULL_RES == DOUBLE || VERSION_1_FULL_RES == I32 || VERSION_1_FULL_RES == I64 || VERSION_1_FULL_RES == STR)
            throw new RuntimeException("Invalid FullResSerializer.CUR_VERSION. Please increment until this exception does not happen.");
    }

    public static final Charset DEFAULT_CHARSET = CharsetUtil.UTF_8;

    private Constants() {}
}
