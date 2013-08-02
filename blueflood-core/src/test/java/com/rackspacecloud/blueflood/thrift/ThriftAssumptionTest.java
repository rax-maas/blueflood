package com.rackspacecloud.blueflood.thrift;

import com.rackspacecloud.blueflood.io.AstyanaxReader;
import junit.framework.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import telescope.thrift.UnitEnum;

// todo: CM_SPECIFIC
public class ThriftAssumptionTest {
    
    // if this test ever breaks, we know that something change in blueflood upstream.
    @Test
    public void testUnknownAsString() {
        String astyanaxReaderConstant = (String)Whitebox.getInternalState(AstyanaxReader.getInstance(), "UNKNOWN_UNIT");
        Assert.assertEquals(UnitEnum.UNKNOWN.toString().toLowerCase(), astyanaxReaderConstant);
    }
}
