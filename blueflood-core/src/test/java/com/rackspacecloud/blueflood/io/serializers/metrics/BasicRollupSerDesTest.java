package com.rackspacecloud.blueflood.io.serializers.metrics;

import com.rackspacecloud.blueflood.io.serializers.Serializers;
import com.rackspacecloud.blueflood.types.BasicRollup;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.SimpleNumber;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by shin4590 on 3/22/16.
 */
public class BasicRollupSerDesTest {

    @Test
    public void testRollupSerializationLargeCounts() throws IOException {
        Points<BasicRollup> rollupGroup = new Points<BasicRollup>();
        BasicRollup startingRollup = new BasicRollup();
        startingRollup.setCount(500);
        rollupGroup.add(new Points.Point<BasicRollup>(123456789L, startingRollup));

        for (int rollupCount = 0; rollupCount < 500; rollupCount++) {
            Points<SimpleNumber> input = new Points<SimpleNumber>();
            for (int fullResCount = 0; fullResCount < 500; fullResCount++) {
                input.add(new Points.Point<SimpleNumber>(123456789L + fullResCount, new SimpleNumber(fullResCount + fullResCount * 3)));
            }
            BasicRollup basicRollup = BasicRollup.buildRollupFromRawSamples(input);
            Points<BasicRollup> rollups = new Points<BasicRollup>();
            rollups.add(new Points.Point<BasicRollup>(123456789L , basicRollup));
            BasicRollup groupRollup = BasicRollup.buildRollupFromRollups(rollups);
            rollupGroup.add(new Points.Point<BasicRollup>(123456789L, groupRollup));
        }

        BasicRollup r = BasicRollup.buildRollupFromRollups(rollupGroup);

        // serialization was broken.
        ByteBuffer bb = Serializers.serializerFor(BasicRollup.class).toByteBuffer(r);
        Assert.assertEquals(r, Serializers.serializerFor(BasicRollup.class).fromByteBuffer(bb));
    }
}
