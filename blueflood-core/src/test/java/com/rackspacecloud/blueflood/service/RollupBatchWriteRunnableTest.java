package com.rackspacecloud.blueflood.service;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.ArrayList;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class RollupBatchWriteRunnableTest {

    @Test
    public void runSendsRollupsToWrierAndDecrementsCount() throws ConnectionException {

        // given
        ArrayList<SingleRollupWriteContext> wcs = new ArrayList<SingleRollupWriteContext>();
        ArrayList<SingleRollupWriteContext> wcs2 = new ArrayList<SingleRollupWriteContext>();
        RollupExecutionContext ctx = mock(RollupExecutionContext.class);
        AstyanaxWriter writer = mock(AstyanaxWriter.class);
        RollupBatchWriteRunnable rbwr = new RollupBatchWriteRunnable(wcs, ctx, writer);

        // when
        rbwr.run();

        // then
        verify(writer).insertRollups(Matchers.<ArrayList<SingleRollupWriteContext>>any());
        verifyNoMoreInteractions(writer);
        verify(ctx).decrementWriteCounter(anyLong());
        verifyNoMoreInteractions(ctx);
    }
}
