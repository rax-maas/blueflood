package com.rackspacecloud.blueflood.io.astyanax;

import com.codahale.metrics.Timer;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.query.RowQuery;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.DelayedLocatorIO;
import com.rackspacecloud.blueflood.io.Instrumentation;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;
import com.rackspacecloud.blueflood.types.Locator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class ADelayedLocatorIO implements DelayedLocatorIO {

    private static final Logger LOG = LoggerFactory.getLogger(ADelayedLocatorIO.class);

    @Override
    public void insertLocator(Granularity g, int slot, Locator locator) throws IOException {

        Timer.Context timer = Instrumentation.getWriteTimerContext(CassandraModel.CF_METRICS_DELAYED_LOCATOR_NAME);

        try {
            MutationBatch mutationBatch = AstyanaxIO.getKeyspace().prepareMutationBatch();
            AstyanaxWriter.getInstance().insertDelayedLocator(g, slot, locator, mutationBatch);
            mutationBatch.execute();
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            timer.stop();
        }
    }

    @Override
    public Collection<Locator> getLocators(SlotKey slotKey) throws IOException {

        Timer.Context ctx = Instrumentation.getReadTimerContext(CassandraModel.CF_METRICS_DELAYED_LOCATOR_NAME);

        try {
            RowQuery<SlotKey, Locator> query = AstyanaxIO.getKeyspace()
                    .prepareQuery(CassandraModel.CF_METRICS_DELAYED_LOCATOR)
                    .getKey(slotKey);
            return query.execute().getResult().getColumnNames();
        } catch (NotFoundException e) {
            Instrumentation.markNotFound(CassandraModel.CF_METRICS_DELAYED_LOCATOR_NAME);
            return Collections.emptySet();
        } catch (ConnectionException ex) {
            Instrumentation.markPoolExhausted();
            Instrumentation.markReadError();
            LOG.error("Connection exception during ADelayedLocatorIO.getLocators(" + slotKey.toString() + ")", ex);
            throw new IOException("Error reading delayed locators", ex);
        } finally {
            ctx.stop();
        }
    }
}
