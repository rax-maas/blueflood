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

package com.rackspacecloud.blueflood.types;

import java.io.IOException;

abstract public class Rollup {
    public static enum Type {
        HISTOGRAM("HISTOGRAM"),
        BASIC_STATS("BASIC_STATS");

        private String name;

        private Type(String name) {
            this.name = name;
        }
    }

    abstract public void computeFromSimpleMetrics(Points<SimpleNumber> input) throws IOException;

    abstract public void computeFromRollups(Points<? extends Rollup> input) throws IOException;

    public static Rollup buildRollupFromRawSamples(Points<SimpleNumber> input, Type rollupType) throws IOException {
        if (rollupType.equals(Type.BASIC_STATS)) {
            return BasicRollup.buildRollupFromRawSamples(input);
        } else if (rollupType.equals(Type.HISTOGRAM)) {
            return HistogramRollup.buildRollupFromRawSamples(input);
        } else {
            throw new IOException("No other rollup type implemented");
        }
    }

    public static Rollup buildRollupFromRollups(Points<? extends Rollup> input, Type rollupType) throws IOException {
        if (rollupType.equals(Type.BASIC_STATS)) {
            return BasicRollup.buildRollupFromRollups(input);
        } else if (rollupType.equals(Type.HISTOGRAM)) {
            return HistogramRollup.buildRollupFromRollups(input);
        } else {
            throw new IOException("No other rollup type implemented");
        }
    }
}
