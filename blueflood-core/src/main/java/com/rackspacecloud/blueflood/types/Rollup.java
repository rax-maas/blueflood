/*
 * Copyright 2015 Rackspace
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

public interface Rollup {
    
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
            return TimerRollup.buildRollupFromTimerRollups(input);
        }
    };
    
    public static final Type<SimpleNumber, CounterRollup> CounterFromRaw = new Type<SimpleNumber, CounterRollup>() {
        @Override
        public CounterRollup compute(Points<SimpleNumber> input) throws IOException {
            return CounterRollup.buildRollupFromRawSamples(input);
        }
    };
    
    public static final Type<CounterRollup, CounterRollup> CounterFromCounter = new Type<CounterRollup, CounterRollup>() {
        @Override
        public CounterRollup compute(Points<CounterRollup> input) throws IOException {
            return CounterRollup.buildRollupFromCounterRollups(input);
        }
    };
    
    public static final Type<SimpleNumber, GaugeRollup> GaugeFromRaw = new Type<SimpleNumber, GaugeRollup>() {
        @Override
        public GaugeRollup compute(Points<SimpleNumber> input) throws IOException {
            return GaugeRollup.buildFromRawSamples(input);
        }
    };
    
    public static final Type<GaugeRollup, GaugeRollup> GaugeFromGauge = new Type<GaugeRollup, GaugeRollup>() {
        @Override
        public GaugeRollup compute(Points<GaugeRollup> input) throws IOException {
            return GaugeRollup.buildFromGaugeRollups(input);
        }
    };
    
    public static final Type<SetRollup, SetRollup> SetFromSet = new Type<SetRollup, SetRollup>() {
        @Override
        public SetRollup compute(Points<SetRollup> input) throws IOException {
            return SetRollup.buildRollupFromSetRollups(input);
        }
    };

    // Tells whether or not data exists
    public Boolean hasData();
    public RollupType getRollupType();
}
