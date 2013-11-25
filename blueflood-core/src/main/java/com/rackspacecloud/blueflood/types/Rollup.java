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
    
    public abstract static class Type<I extends Rollup, O extends Rollup> {
        public abstract O compute(Points<I> input) throws IOException;
    }
    
    public static final Type<SimpleNumber, BasicRollup> BasicFromRaw = new Type<SimpleNumber, BasicRollup>() {
        @Override
        public BasicRollup compute(Points<SimpleNumber> input) throws IOException {
            return BasicRollup.buildRollupFromRawSamples(input);
        }
    };
    
    public static final Type<BasicRollup, BasicRollup> BasicFromBasic = new Type<BasicRollup, BasicRollup>() {
        @Override
        public BasicRollup compute(Points<BasicRollup> input) throws IOException {
            return BasicRollup.buildRollupFromRollups(input);
        }
    };
    
    public static final Type<SimpleNumber, HistogramRollup> HistogramFromRaw = new Type<SimpleNumber, HistogramRollup>() {
        @Override
        public HistogramRollup compute(Points<SimpleNumber> input) throws IOException {
            return HistogramRollup.buildRollupFromRawSamples(input);
        }
    };
    
    public static final Type<HistogramRollup, HistogramRollup> HistogramFromHistogram = new Type<HistogramRollup, HistogramRollup>() {
        @Override
        public HistogramRollup compute(Points<HistogramRollup> input) throws IOException {
            return HistogramRollup.buildRollupFromRollups(input);
        }
    };
    
    public static final Type<TimerRollup, TimerRollup> TimerFromTimer = new Type<TimerRollup, TimerRollup>() {
        @Override
        public TimerRollup compute(Points<TimerRollup> input) throws IOException {
            return TimerRollup.buildRollupFromRollups(input);
        }
    };
    
    // todo: remove this method, convert class to an interface.
    abstract public long getCount();
}
