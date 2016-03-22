package com.rackspacecloud.blueflood.io.serializers.astyanax;

import com.rackspacecloud.blueflood.types.Locator;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * A class to test LocatorSerializer
 */
public class LocatorSerializerTest {

    @Test
    public void testLocatorDeserializer() throws UnsupportedEncodingException {
        String locatorString = "ac76PeGPSR.entZ4MYd1W.chJ0fvB5Ao.mzord.truncated";
        ByteBuffer bb = ByteBuffer.wrap(locatorString.getBytes("UTF-8"));
        Locator locatorFromString = Locator.createLocatorFromDbKey(locatorString);
        Locator locatorDeserialized = LocatorSerializer.get().fromByteBuffer(bb);
        Assert.assertEquals("Locator did not match after deserialization",
                locatorFromString.toString(), locatorDeserialized.toString());
    }
}
