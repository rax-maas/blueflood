/*
 * Copyright (c) 2016 Rackspace.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspacecloud.blueflood.utils;

import com.rackspacecloud.blueflood.io.AbstractMetricsRW;
import com.rackspacecloud.blueflood.io.IOContainer;
import com.rackspacecloud.blueflood.types.RollupType;

/**
 * A utility class containing helper methods related to rollups
 */
public class RollupUtils {

    /**
     * For a particular rollupType, determine what is the proper class to do
     * read/write with and return that
     *
     * @param rollupType
     * @return
     */
    public static AbstractMetricsRW getMetricsRWForRollupType(RollupType rollupType) {
        if (rollupType == null || rollupType == RollupType.BF_BASIC ) {
            return IOContainer.fromConfig().getBasicMetricsRW();
        } else {
            return IOContainer.fromConfig().getPreAggregatedMetricsRW();
        }
    }
}
