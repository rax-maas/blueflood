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
    
    public static final Type<BluefloodTimerRollup, BluefloodTimerRollup> TimerFromTimer = new Type<BluefloodTimerRollup, BluefloodTimerRollup>() {
        @Override
        public BluefloodTimerRollup compute(Points<BluefloodTimerRollup> input) throws IOException {
            return BluefloodTimerRollup.buildRollupFromTimerRollups(input);
        }
    };
    
    public static final Type<SimpleNumber, BluefloodCounterRollup> CounterFromRaw = new Type<SimpleNumber, BluefloodCounterRollup>() {
        @Override
        public BluefloodCounterRollup compute(Points<SimpleNumber> input) throws IOException {
            return BluefloodCounterRollup.buildRollupFromRawSamples(input);
        }
    };
    
    public static final Type<BluefloodCounterRollup, BluefloodCounterRollup> CounterFromCounter = new Type<BluefloodCounterRollup, BluefloodCounterRollup>() {
        @Override
        public BluefloodCounterRollup compute(Points<BluefloodCounterRollup> input) throws IOException {
            return BluefloodCounterRollup.buildRollupFromCounterRollups(input);
        }
    };
    
    public static final Type<SimpleNumber, BluefloodGaugeRollup> GaugeFromRaw = new Type<SimpleNumber, BluefloodGaugeRollup>() {
        @Override
        public BluefloodGaugeRollup compute(Points<SimpleNumber> input) throws IOException {
            return BluefloodGaugeRollup.buildFromRawSamples(input);
        }
    };
    
    public static final Type<BluefloodGaugeRollup, BluefloodGaugeRollup> GaugeFromGauge = new Type<BluefloodGaugeRollup, BluefloodGaugeRollup>() {
        @Override
        public BluefloodGaugeRollup compute(Points<BluefloodGaugeRollup> input) throws IOException {
            return BluefloodGaugeRollup.buildFromGaugeRollups(input);
        }
    };
    
    public static final Type<BluefloodSetRollup, BluefloodSetRollup> SetFromSet = new Type<BluefloodSetRollup, BluefloodSetRollup>() {
        @Override
        public BluefloodSetRollup compute(Points<BluefloodSetRollup> input) throws IOException {
            return BluefloodSetRollup.buildRollupFromSetRollups(input);
        }
    };

    // Tells whether or not data exists
    public Boolean hasData();
    public RollupType getRollupType();
}
