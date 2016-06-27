/*
 * Copyright 2016 Rackspace
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

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Verify BasicRollup sub-metrics calculation.
 */
public class BasicRollupTest {

    private static final double EPSILON = 0.5;

    private BasicRollup createFromPoints( int count ) throws IOException {

        Points<SimpleNumber> points = new Points<SimpleNumber>();

        for( int i = 1; i <= count; i++ ) {

            points.add( new Points.Point<SimpleNumber>( i, new SimpleNumber( 10 * i ) ) );
        }

        return BasicRollup.buildRollupFromRawSamples( points );
    }

    @Test
    public void buildRollupFromRawSamples() throws IOException {

        BasicRollup rollup = createFromPoints( 5 );

        assertEquals( "count is equal", 5, rollup.getCount() );
        assertEquals( "average is equal", 30L, rollup.getAverage().toLong() );
        assertEquals( "variance is equal", 200, rollup.getVariance().toDouble(), EPSILON );
        assertEquals( "minValue is equal", 10, rollup.getMinValue().toLong() );
        assertEquals( "maxValue is equal", 50, rollup.getMaxValue().toLong() );
        assertEquals( "sum is equal", 150, rollup.getSum(), EPSILON );
    }

    @Test
    public void buildRollupFromRollups() throws IOException {

        Points<BasicRollup> rollups = new Points<BasicRollup>() {{

            add( new Points.Point<BasicRollup>( 1, createFromPoints( 1 ) ) );
            add( new Points.Point<BasicRollup>( 2, createFromPoints( 2 ) ) );
            add( new Points.Point<BasicRollup>( 3, createFromPoints( 3 ) ) );
        }};

        BasicRollup rollup = BasicRollup.buildRollupFromRollups( rollups );

        assertEquals( "count is equal", 6, rollup.getCount() );
        assertEquals( "average is equal", 16L, rollup.getAverage().toLong() );
        assertEquals( "variance is equal", 55.55, rollup.getVariance().toDouble(), EPSILON );
        assertEquals( "minValue is equal", 10, rollup.getMinValue().toLong() );
        assertEquals( "maxValue is equal", 30, rollup.getMaxValue().toLong() );
        assertEquals( "sum is equal", 100, rollup.getSum(), EPSILON );
    }
}
