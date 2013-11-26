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

package com.rackspacecloud.blueflood.utils;

import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.io.AstyanaxIO;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.*;

import java.util.HashMap;
import java.util.Map;

public class RollupTestUtils {
    public static void generateRollups(Locator locator, long from, long to, Granularity destGranularity) throws Exception {
        if (destGranularity == Granularity.FULL) {
            throw new Exception("Can't roll up to FULL");
        }

        Map<Long, Rollup> rollups = new HashMap<Long, Rollup>();
        for (Range range : Range.rangesForInterval(destGranularity, from, to)) {
            Points<SimpleNumber> input = AstyanaxReader.getInstance().getSimpleDataToRoll(locator, range);
            BasicRollup basicRollup = BasicRollup.buildRollupFromRawSamples(input);
            rollups.put(range.getStart(), basicRollup);
        }

        ColumnFamily<Locator, Long> destCF = AstyanaxIO.getColumnFamilyMapper().get(destGranularity);
        AstyanaxWriter.getInstance().insertRollups(locator, rollups, destCF);
    }
}
