package com.rackspacecloud.blueflood.service;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.ArrayList;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.*;

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

    @Test
    public void connectionExceptionMarksUnsuccessful() throws ConnectionException {

        // given
        ArrayList<SingleRollupWriteContext> wcs = new ArrayList<SingleRollupWriteContext>();
        ArrayList<SingleRollupWriteContext> wcs2 = new ArrayList<SingleRollupWriteContext>();
        RollupExecutionContext ctx = mock(RollupExecutionContext.class);
        AstyanaxWriter writer = mock(AstyanaxWriter.class);
        Throwable cause = new ConnectionException("exception for testing purposes") { };
        doThrow(cause).when(writer).insertRollups(
                Matchers.<ArrayList<SingleRollupWriteContext>>any());
        RollupBatchWriteRunnable rbwr = new RollupBatchWriteRunnable(wcs, ctx, writer);

        // when
        rbwr.run();

        // then
        verify(writer).insertRollups(Matchers.<ArrayList<SingleRollupWriteContext>>any());
        verifyNoMoreInteractions(writer);
        verify(ctx).markUnsuccessful(Matchers.<Throwable>any());
        verify(ctx).decrementWriteCounter(anyLong());
        verifyNoMoreInteractions(ctx);
    }

    @Test
    public void otherExceptionBreaksEverything() throws ConnectionException {

        // given
        ArrayList<SingleRollupWriteContext> wcs = new ArrayList<SingleRollupWriteContext>();
        ArrayList<SingleRollupWriteContext> wcs2 = new ArrayList<SingleRollupWriteContext>();
        RollupExecutionContext ctx = mock(RollupExecutionContext.class);
        AstyanaxWriter writer = mock(AstyanaxWriter.class);
        Throwable cause = new UnsupportedOperationException("exception for testing purposes");
        doThrow(cause).when(writer).insertRollups(
                Matchers.<ArrayList<SingleRollupWriteContext>>any());
        RollupBatchWriteRunnable rbwr = new RollupBatchWriteRunnable(wcs, ctx, writer);

        // when
        Throwable caught = null;
        try {
            rbwr.run();
        } catch (Throwable t) {
            caught = t;
        }

        // then
        assertNotNull(caught);
        assertSame(cause, caught);
        verify(writer).insertRollups(Matchers.<ArrayList<SingleRollupWriteContext>>any());
        verifyNoMoreInteractions(writer);
        verifyZeroInteractions(ctx);
    }
}
