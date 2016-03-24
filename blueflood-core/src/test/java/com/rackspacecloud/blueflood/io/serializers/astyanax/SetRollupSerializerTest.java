package com.rackspacecloud.blueflood.io.serializers.astyanax;

import com.google.common.collect.Sets;
import com.rackspacecloud.blueflood.io.serializers.Serializers;
import com.rackspacecloud.blueflood.types.BluefloodSetRollup;
import com.rackspacecloud.blueflood.utils.Rollups;
import junit.framework.Assert;
import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Set;

public class SetRollupSerializerTest {

    private static Set set1 = Sets.newHashSet(new Integer(10), new Integer(20), new Integer(30));
    private static Set set2 = Sets.newHashSet("aaa", "bbb", "ccc");
    private static Set set3 = Sets.newHashSet("11111", "22222", "33333");

    @Test
    public void testSerializerDeserializerV1Test() throws Exception {
        BluefloodSetRollup setRollup1 = new BluefloodSetRollup().withObject(set1);
        BluefloodSetRollup setRollup2 = new BluefloodSetRollup().withObject(set2);
        BluefloodSetRollup setRollup3 = new BluefloodSetRollup().withObject(set3);

        BluefloodSetRollup setsRollup = BluefloodSetRollup.buildRollupFromSetRollups(
                Rollups.asPoints(BluefloodSetRollup.class, System.currentTimeMillis(), 300, setRollup1, setRollup2, setRollup3));
        Assert.assertEquals(3, setsRollup.getCount());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(Base64.encodeBase64(Serializers.setRollupInstance.toByteBuffer(setRollup1).array()));
        baos.write("\n".getBytes());
        baos.write(Base64.encodeBase64(Serializers.setRollupInstance.toByteBuffer(setRollup2).array()));
        baos.write("\n".getBytes());
        baos.write(Base64.encodeBase64(Serializers.setRollupInstance.toByteBuffer(setRollup3).array()));
        baos.write("\n".getBytes());
        baos.write(Base64.encodeBase64(Serializers.setRollupInstance.toByteBuffer(setsRollup).array()));
        baos.write("\n".getBytes());
        baos.close();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(baos.toByteArray())));

        ByteBuffer bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
        BluefloodSetRollup deserializedSet1 = Serializers.serializerFor(BluefloodSetRollup.class).fromByteBuffer(bb);
        Assert.assertEquals(setRollup1, deserializedSet1);

        bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
        BluefloodSetRollup deserializedSet2 = Serializers.serializerFor(BluefloodSetRollup.class).fromByteBuffer(bb);
        Assert.assertEquals(setRollup2, deserializedSet2);

        bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
        BluefloodSetRollup deserializedSet3 = Serializers.serializerFor(BluefloodSetRollup.class).fromByteBuffer(bb);
        Assert.assertEquals(setRollup3, deserializedSet3);

        bb = ByteBuffer.wrap(Base64.decodeBase64(reader.readLine().getBytes()));
        BluefloodSetRollup deserializedSet4 = Serializers.serializerFor(BluefloodSetRollup.class).fromByteBuffer(bb);
        Assert.assertEquals(setsRollup, deserializedSet4);

        Assert.assertFalse(deserializedSet1.equals(deserializedSet2));
    }
}
