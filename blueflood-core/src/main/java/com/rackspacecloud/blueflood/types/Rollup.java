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
    
    // todo: these classes and instance can be moved into a static Computations holder.
    
    public abstract static class Type<T extends Rollup> {
        public abstract T buildRollupFromRawSamples(Points<SimpleNumber> input) throws IOException;
        public abstract T buildRollupFromRollups(Points<T> input) throws IOException;
    }
    
    public static final Type BasicType = new Type<BasicRollup>() {
        
        @Override
        public BasicRollup buildRollupFromRawSamples(Points<SimpleNumber> input) throws IOException {
            return BasicRollup.buildRollupFromRawSamples(input);
        }

        @Override
        public BasicRollup buildRollupFromRollups(Points<BasicRollup> input) throws IOException {
            return BasicRollup.buildRollupFromRollups(input);
        }
    };
    
    public static final Type HistogramType = new Type<HistogramRollup>() {
        
        @Override
        public HistogramRollup buildRollupFromRawSamples(Points<SimpleNumber> input) throws IOException {
            return HistogramRollup.buildRollupFromRawSamples(input);
        }

        @Override
        public HistogramRollup buildRollupFromRollups(Points<HistogramRollup> input) throws IOException {
            return HistogramRollup.buildRollupFromRollups(input);
        }
    };
    
    abstract public long getCount();
}
